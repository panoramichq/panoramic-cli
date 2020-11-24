def install
  venv = virtualenv_create(libexec, "python3")
  venv.pip_install resources
  venv.pip_install buildpath
  (bin/"pano").write_env_script "#{libexec}/bin/pano", :RUNNING_UNDER_HOMEBREW => "1"
end

test do
  false
end
