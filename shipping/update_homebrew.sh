#!/bin/bash

generate_homebrew_formula() {
  rm -fr brewout
  mkdir -p brewout
  shipping/ppfg.sh -f -c shipping/ppfg -o brewout $(python shipping/generate-versions-file.py)
  cat brewout/panoramic-cli.rb
}

setup_git() {
  git config --global user.email "travis@travis-ci.org"
  git config --global user.name "Travis CI"
}

pull_homebrew_files() {
  rm -fr .homebrew_repo
  git clone https://github.com/panoramichq/homebrew-brew.git .homebrew_repo
}

update_homebrew_formula() {
  cd .homebrew_repo || true
  git checkout master || true
  cd ..
  rm -fr .homebrew_repo/Formula/panoramic-cli.rb
  cp brewout/panoramic-cli.rb .homebrew_repo/Formula/panoramic-cli.rb
  cd .homebrew_repo
  git add Formula/panoramic-cli.rb
  git commit --message "Travis build: $TRAVIS_BUILD_NUMBER"
}

upload_files() {
  cd .homebrew_repo || true
  git push origin master
}

generate_homebrew_formula
setup_git
pull_homebrew_files
update_homebrew_formula
upload_files
