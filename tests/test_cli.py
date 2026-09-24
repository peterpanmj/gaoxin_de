from click.testing import CliRunner

from saleor_analytics.cli import cli


def test_help():
    result = CliRunner().invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "doctor" in result.output
    assert "export-artifacts" in result.output


def test_invalid_configuration(tmp_path):
    config = tmp_path / "bad.toml"
    config.write_text('root = "data"\npage_size = 101\n')
    result = CliRunner().invoke(cli, ["--config", str(config), "doctor"])
    assert result.exit_code != 0
