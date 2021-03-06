#!/bin/bash

generate_homebrew_formula() {
  rm -fr brewout
  mkdir -p brewout
  python shipping/wait_for_pypi.py pano $(python shipping/generate-versions-file.py)
  shipping/ppfg.sh -f -c shipping/ppfg -o brewout $(python shipping/generate-versions-file.py)
}

setup_git() {
  git config --global user.email "panoramic-ci@users.noreply.github.com"
  git config --global user.name "Panoramic CI"
}

update_homebrew_formula() {
  PACKAGE_VERSION=$(python setup.py --version)

  cd .homebrew_repo || true
  git checkout master || true
  cd ..
  mkdir -p .homebrew_repo/Formula

  # Check for a pre-release, if this returns False then we update the main release file
  IS_PRERELEASE=$(echo $PACKAGE_VERSION | python -c 'import packaging.version as v;print(v.parse(input()).is_prerelease)')

  rm -fr .homebrew_repo/Formula/pano@"$PACKAGE_VERSION".rb
  CLASS_NAME=$(ruby shipping/string_to_class.rb pano@"$PACKAGE_VERSION")
  echo $CLASS_NAME
  sed -e "s/Pano/$CLASS_NAME/" brewout/pano.rb > .homebrew_repo/Formula/pano@"$PACKAGE_VERSION".rb

  if [[ "$IS_PRERELEASE" == "False" ]]; then
    rm -fr .homebrew_repo/Formula/pano.rb
    cp brewout/pano.rb .homebrew_repo/Formula/pano.rb
  fi

  cd .homebrew_repo
  git add Formula/pano@"$PACKAGE_VERSION".rb

  if [[ "$IS_PRERELEASE" == "False" ]]; then
    git add Formula/pano.rb
  fi

  git commit --message "Panoramic CLI build: $GITHUB_RUN_NUMBER"
  cd ..
}

upload_files() {
  cd .homebrew_repo || true
  if [ -f "Formula/pano.rb" ]; then
    git push origin master
  fi
}

generate_homebrew_formula
setup_git
update_homebrew_formula
upload_files
