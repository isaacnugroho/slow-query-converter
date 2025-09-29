#[cfg(test)]
mod tests {
    use std::process::Command;

    #[test]
    fn test_help_output() {
        let output = Command::new("cargo")
            .args(&["run", "--", "--help"])
            .output()
            .expect("Failed to execute command");

        assert!(output.status.success());
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("slow-query-converter"));
        assert!(stdout.contains("--input"));
        assert!(stdout.contains("--output"));
    }

    #[test]
    fn test_version_output() {
        let output = Command::new("cargo")
            .args(&["run", "--", "--version"])
            .output()
            .expect("Failed to execute command");

        assert!(output.status.success());
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("slow-query-converter"));
    }

    #[test]
    fn test_invalid_input() {
        let output = Command::new("cargo")
            .args(&["run", "--", "--input", "nonexistent.log"])
            .output()
            .expect("Failed to execute command");

        assert!(!output.status.success());
    }
}
