import nox

nox.options.default_venv_backend = "uv"
nox.options.reuse_existing_virtualenvs = True


@nox.session
def update(session):
    """Sync HubSpot deals data to Google Sheets."""
    session.install("-r", "requirements.txt")
    session.run("python", "sync_hubspot_to_sheets.py")
