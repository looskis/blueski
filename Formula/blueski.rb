class Blueski < Formula
  desc "AppleScript-only macOS Messages send/receive daemon"
  homepage "https://github.com/raz-team/blueski"
  license "MIT"
  head "https://github.com/raz-team/blueski.git", branch: "main"

  depends_on "rust" => :build
  depends_on macos: :monterey

  def install
    system "cargo", "install", *std_cargo_args
  end

  service do
    run [opt_bin/"blueski", "run"]
    keep_alive true
    log_path var/"log/blueski.log"
    error_log_path var/"log/blueski.err"
    environment_variables PATH: std_service_path_env
  end

  test do
    assert_match "AppleScript-only", shell_output("#{bin}/blueski --help")
  end
end
