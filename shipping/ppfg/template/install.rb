def install
  venv = virtualenv_create(libexec, "python3")
  # Fix issues with Big Sur version detection
  inreplace "#{libexec}/lib/python3.8/site-packages/pkg_resources/_vendor/packaging/version.py",
    "match = self._regex.search(version)",
    "match = self._regex.search(str(version))"
  venv.pip_install resources
  venv.pip_install buildpath
  (bin/"pano").write_env_script "#{libexec}/bin/pano", :RUNNING_UNDER_HOMEBREW => "1"
end

test do
  false
end
