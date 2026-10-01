from typer.testing import CliRunner

from cli.main import app

runner = CliRunner()


def test_pipeline_rebuild_dry_run():
    result = runner.invoke(app, ["pipeline", "rebuild", "--template-version", "2.0", "--dry-run"])
    assert result.exit_code == 0
    assert "Dry-run active" in result.stdout


def test_pipeline_rebuild_standard():
    result = runner.invoke(app, ["pipeline", "rebuild", "--template-version", "2.0"])
    assert result.exit_code == 0
    assert "Rebuild completed successfully" in result.stdout
