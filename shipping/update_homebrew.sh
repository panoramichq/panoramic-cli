#!/bin/bash

generate_homebrew_formula() {
  rm -fr brewout
  mkdir -p brewout
  shipping/ppfg.sh -f -c shipping/ppfg -o brewout $(python shipping/generate-versions-file.py)
}

setup_git() {
  git config --global user.email "27856297+dependabot-preview[bot]@users.noreply.github.com"
  git config --global user.name "GitHub Actions"
}

update_homebrew_formula() {
  cd .homebrew_repo || true
  git checkout master || true
  cd ..
  rm -fr .homebrew_repo/Formula/panoramic-cli.rb
  cp brewout/panoramic-cli.rb .homebrew_repo/Formula/panoramic-cli.rb
  cd .homebrew_repo
  git add Formula/panoramic-cli.rb
  git commit --message "GitHub Actions build: $GITHUB_RUN_NUMBER"
  cd ..
}

upload_files() {
  cd .homebrew_repo || true
  git push origin master
}

generate_homebrew_formula
setup_git
update_homebrew_formula
upload_files
