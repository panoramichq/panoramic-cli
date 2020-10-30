def install
  virtualenv_install_with_resources :using => "python3"
  rm_rf bin/"pano"
  (bin/"pano").write_env_script "#{libexec}/bin/pano", :RUNNING_UNDER_HOMEBREW => "1"
end

test do
  false
end
