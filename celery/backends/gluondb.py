from datetime import datetime

from celery import states
from celery.backends.base import BaseDictBackend
from celery.exceptions import ImproperlyConfigured
from celery.utils.timeutils import maybe_timedelta


def _web2py_installed():
    try:
        import gluon
    except ImportError:
        raise ImproperlyConfigured(
            "The gluondb result backend requires web2py to be installed."
            "See http://pypi.python.org/pypi/web2py")
    return gluon
_web2py_installed()


class DatabaseBackend(BaseDictBackend):
    """The database result backend."""

    def __init__(self, dburi=None, expires=None,
            engine_options=None, **kwargs):
        super(DatabaseBackend, self).__init__(**kwargs)
        self.expires = maybe_timedelta(self.prepare_expires(expires))
        self.dburi = dburi or self.app.conf.CELERY_RESULT_DBURI
        self.engine_options = dict(engine_options or {},
                        **self.app.conf.CELERY_RESULT_ENGINE_OPTIONS or {})
        if not self.dburi:
            raise ImproperlyConfigured(
                    "Missing connection string! Do you have "
                    "CELERY_RESULT_DBURI set to a real value?")
