import nox

nox.options.default_venv_backend = "uv"
nox.options.reuse_existing_virtualenvs = True


@nox.session
def download(session):
    """Download latest deals from HubSpot."""
    session.install("-r", "requirements.txt")
    session.run("python", "scripts/download_data.py")


@nox.session
def test(session):
    """Run tests on downloaded data."""
    session.install("-r", "requirements.txt")
    session.run("pytest", "tests/", "-v")


@nox.session
def update(session):
    """Run tests, then upload downloaded deals and KPI MAU table."""
    session.install("-r", "requirements.txt")
    session.run("pytest", "tests/", "-v")
    session.run("python", "scripts/upload_data.py")


@nox.session(name="download-and-update")
def download_and_update(session):
    """Download, test, and upload in sequence."""
    session.notify("download")
    session.notify("update")
