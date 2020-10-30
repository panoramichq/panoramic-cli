#!/bin/bash

setup_git() {
  git config --global user.email "travis@travis-ci.org"
  git config --global user.name "Travis CI"
}

pull_homebrew_files() {
  rm -fr .homebrew_repo
  git clone git@github.com:panoramichq/homebrew-brew.git .homebrew-git
}

update_homebrew_formula() {
  git checkout -b master
  rm -fr .homebrew_repo/Formula/panoramic-cli.rb
  cp brewout/panoramic-cli.rb .homebrew_repo/Formula/panoramic-cli.rb
  cd .homebrew_repo
  git Formula/panoramic-cli.rb
  git commit --message "Travis build: $TRAVIS_BUILD_NUMBER"
}

upload_files() {
  git push master
}

setup_git
pull_homebrew_files
update_homebrew_formula
upload_files
