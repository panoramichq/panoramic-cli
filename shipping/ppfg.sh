#!/bin/bash

readonly VERSION="0.2.0"

readonly POET_VERSION="0.10.0"

# argument and option analysis
show_usage() {
  cat << EOS
Usage: $(basename "$0") [OPTIONS] VERSION

  Automatically generates python package formula file

Options:
  -c PATH  Path of config directory [default: ~/.ppfg]
  -o PATH  Path of output directory [default: .]
  -f       Enable overwriting of the output file
  -d       Enable debug mode
  -V       Show the version and exit
  -h       Show this message and exit
EOS
  exit 0
}

show_version() {
  echo "$(basename "$0") $VERSION"
  exit 0
}

usage_error() {
  local _error_message=$1
  cat 1>&2 << EOS
$_error_message

Try '$(basename "$0") -h' for help.
EOS
  exit 1
}

# from http://fahdshariff.blogspot.com/2014/02/retrying-commands-in-shell-scripts.html
retry() {
    local -r -i max_attempts="$1"; shift
    local -r cmd="$@"
    local -i attempt_num=1

    until $cmd
    do
        if (( attempt_num == max_attempts ))
        then
            echo "Attempt $attempt_num failed and there are no more attempts left!"
            return 1
        else
            echo "Attempt $attempt_num failed! Trying again in $attempt_num seconds..."
            sleep $(( attempt_num++ ))
        fi
    done
}

outdir="."
config_dif="$HOME/.ppfg"
force=0
debug=0

while getopts :c:o:dfVh OPT
do
  case $OPT in
    c) config_dif=$OPTARG
      ;;
    o) outdir=$OPTARG
      ;;
    d) debug=1
      ;;
    f) force=1
      ;;
    V) show_version
      ;;
    h) show_usage
      ;;
    \?) usage_error "Error: No such option."
      ;;
  esac
done

shift $((OPTIND - 1))

if [ $# -le 0 ]; then
  usage_error "Error: Missing argument 'VERSION'."
elif [ $# -ge 2 ]; then
  _var=( "$@" )
  usage_error "Error: Got unexpected extra arguments: ${_var[*]:1}"
fi
version=$1
if [[ ! $version =~ ^[0-9]+\.[0-9]+.*$ ]]; then
  echo "Error: Invalid version format: $version"
  exit 1
fi

if [ ! -d "$outdir" ]; then
  echo "Error: Output directory not found: $outdir" 1>&2
  exit 1
fi

config="$config_dif/config"
template_dir="$config_dif/template"
if [ ! -f "$config" ]; then
  echo "Error: Config file not found: $config" 1>&2
  exit 1
fi

# Read config file and $package, $desc are assingned values.
. "$config"

if [ -z "$package" ]; then
  echo "Error: 'pacakge' is not set in the config file." 1>&2
  exit 1
fi

outfile="$outdir/$package.rb"
if [ $force -eq 0 ] && [ -e "$outfile" ]; then
  echo "Error: File already exists: $outfile" 1>&2
  exit
fi


# check python
if type python >/dev/null 2>&1; then
  py='python'
elif type python3 >/dev/null 2>&1; then
  py='python3'
else
  echo "Error: Python is required." 1>&2
  exit 1
fi

python_version_major=$(eval $py "-c 'import sys; print(sys.version_info.major)'")
python_version_minor=$(eval $py "-c 'import sys; print(sys.version_info.minor)'")
if [ "$python_version_major" -ne 3 ]; then
  echo "Error: Python 3 is required: $(eval $py -V)" 1>&2
  exit 1
fi


# make temporary directory
tmpdir=$(mktemp -d)
if [ $debug -eq 1 ]; then
  echo "Create temporary directory: $tmpdir"
  echo "The temporary directory is NOT automatically deleted in debug mode."
  echo
else
  trap 'rm -rf $tmpdir' EXIT
  trap 'rm -rf $tmpdir; exit 1' INT PIPE TERM
fi


# processing
echo "Start generating formula file for $package $version"

echo "Create python virtual environments"
eval $py "-m venv $tmpdir/venv"
source "$tmpdir"/venv/bin/activate
pip install -U pip > /dev/null

echo "Install python package"
pip install homebrew-pypi-poet==$POET_VERSION > /dev/null
if ! type poet >/dev/null 2>&1; then
  echo "Error: poet could not be installed." 1>&2
  exit 1
fi
if retry 60 pip install --use-feature=2020-resolver "$package"=="$version"; then
  echo "Error: $package $version could not be installed." 1>&2
  exit 1
fi


echo "Generate formula file using poet"
poet -f "$package"=="$version" > "$tmpdir/generated.rb"
if [ ! -f "$tmpdir/generated.rb" ]; then
  echo "Error: Can not unable to generate a file using post." 1>&2
  exit 1
fi

echo "Rewrite formula file"

cut_text() {
  local _start=$1
  local _end=$2
  head -n "$_end" | tail -n $((_end - _start + 1))
}

add_indent() {
  while IFS= read -r line
  do
    if [ -n "$line" ]; then
      echo "  $line"
    else
      echo
    fi
  done
}

sed -e "s/desc \"Shiny new formula\"/desc \"$desc\"/" \
  "$tmpdir/generated.rb" > "$tmpdir/sed-desc.rb"
head -n 8 "$tmpdir/sed-desc.rb" > "$tmpdir/head.rb"

legnth=$(wc -l "$tmpdir/generated.rb" | awk '{print $1}')
cut_text 11 $((legnth - 9)) < "$tmpdir/generated.rb" > "$tmpdir/resources.rb"

if [ -f "$template_dir/install.rb" ]; then
  echo "Using the template: $template_dir/install.rb"
  add_indent < "$template_dir/install.rb" > "$tmpdir/install.rb"
else
  cat > "$tmpdir/install.rb" << EOS
  def install
    virtualenv_create(libexec, "python3.8")
    virtualenv_install_with_resources
  end

  test do
    false
  end
EOS
fi

if [ -f "$template_dir/depends.rb" ]; then
  echo "Using the template: $template_dir/depends.rb"
  add_indent < "$template_dir/depends.rb" > "$tmpdir/depends.rb"
else
  echo '  depends_on "python@3.8"' > "$tmpdir/depends.rb"
fi
echo >> "$tmpdir/depends.rb"

cat \
  "$tmpdir/head.rb" \
  "$tmpdir/depends.rb" \
  "$tmpdir/resources.rb" \
  "$tmpdir/install.rb" \
  > "$tmpdir/replaced.rb"
echo "end" >> "$tmpdir/replaced.rb"

cp "$tmpdir/replaced.rb" "$outfile"
echo
echo "Generated to $outfile"
