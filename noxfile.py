import nox

nox.options.default_venv_backend = "uv"
nox.options.reuse_existing_virtualenvs = True


@nox.session
def sync(session):
    """Download, clean, and model - the full pipeline."""
    session.install("-r", "requirements.txt")
    session.run("python", "scripts/sync.py")
