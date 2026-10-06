"""
auth.py

Login accounts for the dashboard (weather_app.py).

One row per student in the `users` table of the same TimescaleDB database the
readings live in. The primary key is the student number, which is also the
username.

    Default password   PW_PREFIX + <student_number> + PW_SUFFIX
    Username           the student number

Both parts of that scheme come from the environment (see PASSWORD_PREFIX
below), so no credential pattern lives in this file.

A student can change their own password on the dashboard's Account page. Only a
PBKDF2 hash is stored, never the password itself. Seeding inserts accounts that
do not exist yet and leaves existing rows alone, so re-running seed_users.py
never resets a password somebody has already changed.

Who is signed in lives in the Flask session cookie that Dash already runs on
(needs SECRET_KEY, otherwise a key is generated once and kept next to this
file), so there is no extra service and no session table.
"""

import logging
import os
import secrets

import psycopg2
from flask import session
from werkzeug.security import check_password_hash, generate_password_hash

from config import TIMESCALEDB, required_env

log = logging.getLogger("auth")

# The default-password scheme is read from the environment so it cannot be
# committed. Because usernames are bare student numbers, anyone who knows the
# pattern can derive any account's first password - so it must not be in source.
# Set both before seeding or running the dashboard:
#
#     export PW_PREFIX='...'
#     export PW_SUFFIX='...'
#
PASSWORD_PREFIX = required_env("PW_PREFIX")
PASSWORD_SUFFIX = required_env("PW_SUFFIX")
MIN_PASSWORD_LENGTH = 8

SESSION_STUDENT_NUMBER = "student_number"
SESSION_FULL_NAME = "full_name"

# Environment variable and file used to persist the Flask session secret key.
SECRET_KEY_ENV = "SECRET_KEY"
SECRET_KEY_FILE = ".dashboard_secret_key"

# Connection errors to retry on when talking to Postgres/TimescaleDB.
_CONNECTION_ERRORS = (psycopg2.OperationalError, psycopg2.InterfaceError)


def default_password(student_number):
    """The password every student starts with: PW_PREFIX + number + PW_SUFFIX."""
    return f"{PASSWORD_PREFIX}{clean_student_number(student_number)}{PASSWORD_SUFFIX}"


def clean_student_number(value):
    """Trim a submitted student number. Returns None if it is not a bare number,
    so '4262103 ' works but 'student' does not."""
    text = str(value or "").strip()
    if text and text.isdigit():
        return text
    return None


def secret_key():
    """The key that signs the session cookie. Uses SECRET_KEY from the
    environment when it is set, so every process on the Pi shares one key."""
    configured = os.environ.get(SECRET_KEY_ENV)
    if configured:
        return configured

    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), SECRET_KEY_FILE)
    try:
        with open(path, "r", encoding="utf-8") as handle:
            stored = handle.read().strip()
        if stored:
            return stored
    except OSError:
        pass

    generated = secrets.token_hex(32)
    try:
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(generated)
        os.chmod(path, 0o600)
    except OSError as exc:
        log.warning("Could not save the session key, sessions end on restart: %s", exc)
    return generated


class UsersAdapter:
    """The `users` table: create it, verify a password, change one, seed it.

    Same connection handling as TimescaleDBAdapter -- one autocommit connection
    that reconnects once if it drops, because the Flask server is threaded.
    """

    def __init__(self, db_config=None):
        self._db_config = db_config or TIMESCALEDB
        self._connect()
        self._init_tables()

    def _connect(self):
        self.conn = psycopg2.connect(**self._db_config)
        self.conn.autocommit = True

    def _run(self, fn):
        for attempt in (1, 2):
            try:
                with self.conn.cursor() as cur:
                    return fn(cur)
            except _CONNECTION_ERRORS:
                if attempt == 2:
                    raise
                log.warning("User database connection lost, reconnecting...")
                try:
                    self.conn.close()
                except Exception:
                    pass
                self._connect()

    def _init_tables(self):
        def init(cur):
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    student_number      TEXT PRIMARY KEY,
                    password_hash       TEXT NOT NULL,
                    first_names         TEXT,
                    surname             TEXT,
                    initials            TEXT,
                    using_default_pw    BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    last_login_at       TIMESTAMPTZ
                );
            """)
        self._run(init)

    def close(self):
        self.conn.close()

    # ----------------------------------------------------------
    # READ / WRITE
    # ----------------------------------------------------------
    def get(self, student_number):
        """{'student_number', 'full_name', 'initials', 'using_default_pw'} or None."""
        student_number = clean_student_number(student_number)
        if not student_number:
            return None

        def q(cur):
            cur.execute(
                "SELECT student_number, first_names, surname, initials, using_default_pw "
                "FROM users WHERE student_number = %s",
                (student_number,),
            )
            return cur.fetchone()
        row = self._run(q)
        if not row:
            return None

        first_names = (row[1] or "").strip()
        surname = (row[2] or "").strip()
        return {
            "student_number": row[0],
            "first_names": first_names,
            "surname": surname,
            "full_name": f"{first_names} {surname}".strip() or row[0],
            "initials": (row[3] or "").strip(),
            "using_default_pw": bool(row[4]),
        }

    def verify(self, student_number, password):
        """Returns the user record when the password matches, else None.

        A wrong password and an unknown student number return the same result,
        so the login page cannot be used to enumerate valid student numbers."""
        student_number = clean_student_number(student_number)
        if not student_number or not password:
            return None

        def q(cur):
            cur.execute(
                "SELECT password_hash FROM users WHERE student_number = %s",
                (student_number,),
            )
            return cur.fetchone()
        row = self._run(q)
        if not row or not check_password_hash(row[0], str(password)):
            return None
        return self.get(student_number)

    def set_password(self, student_number, new_password, still_default=False):
        def q(cur):
            cur.execute(
                "UPDATE users SET password_hash = %s, using_default_pw = %s, "
                "updated_at = NOW() WHERE student_number = %s "
                "RETURNING student_number",
                (generate_password_hash(new_password), still_default, student_number),
            )
            return cur.fetchone()
        return self._run(q)[0]

    def record_login(self, student_number):
        def q(cur):
            cur.execute(
                "UPDATE users SET last_login_at = NOW() WHERE student_number = %s",
                (student_number,),
            )
        self._run(q)

    def count(self):
        def q(cur):
            cur.execute("SELECT COUNT(*) FROM users")
            return cur.fetchone()[0]
        return self._run(q)

    def seed(self, students, reset_passwords=False):
        """Add accounts that do not exist yet. Returns {'added', 'existing'}.

        students  iterable of dicts: {'student_number', 'first_names', 'surname', 'initials'}

        Existing rows are left untouched unless reset_passwords is True, so this
        is safe to run on every deploy.
        """
        added = 0
        existing = 0

        for student in students:
            number = clean_student_number(student.get("student_number"))
            if not number:
                continue

            def q(cur, number=number, student=student):
                if reset_passwords:
                    cur.execute(
                        "INSERT INTO users (student_number, password_hash, first_names, "
                        "surname, initials, using_default_pw) "
                        "VALUES (%s, %s, %s, %s, %s, TRUE) "
                        "ON CONFLICT (student_number) DO UPDATE SET password_hash = EXCLUDED.password_hash, "
                        "using_default_pw = TRUE, updated_at = NOW() RETURNING (xmax = 0) AS inserted",
                        (number, generate_password_hash(default_password(number)),
                         student.get("first_names"), student.get("surname"), student.get("initials")),
                    )
                else:
                    cur.execute(
                        "INSERT INTO users (student_number, password_hash, first_names, "
                        "surname, initials, using_default_pw) "
                        "VALUES (%s, %s, %s, %s, %s, TRUE) "
                        "ON CONFLICT (student_number) DO NOTHING "
                        "RETURNING student_number",
                        (number, generate_password_hash(default_password(number)),
                         student.get("first_names"), student.get("surname"), student.get("initials")),
                    )
                return cur.fetchone()
            if self._run(q):
                added += 1
            else:
                existing += 1

        return {"added": added, "existing": existing}


# ----------------------------------------------------------
# Connection singleton
# ----------------------------------------------------------
_users = {"adapter": None, "failed": False}


def get_users(db_config=None, reconnect=False):
    """The shared UsersAdapter, or None when the user database is unreachable.
    Returns None instead of raising so the login page can still render."""
    if db_config is not None or reconnect or _users["adapter"] is None:
        try:
            _users["adapter"] = UsersAdapter(db_config)
            _users["failed"] = False
        except Exception as exc:
            log.error("Cannot reach the user database: %s", exc)
            _users["adapter"] = None
            _users["failed"] = True
    return _users["adapter"]


# ----------------------------------------------------------
# Session helpers, called from inside a Flask request
# ----------------------------------------------------------
def current_student_number():
    """The signed-in student number, or None."""
    try:
        return session.get(SESSION_STUDENT_NUMBER)
    except RuntimeError:
        return None


def signed_in():
    return bool(current_student_number())


def display_name():
    try:
        return session.get(SESSION_FULL_NAME) or current_student_number()
    except RuntimeError:
        return None


def account_count():
    """How many accounts exist. 0 means seed_users.py has not been run yet."""
    users = get_users()
    if users is None:
        return 0
    try:
        return users.count()
    except Exception as exc:
        log.error("Could not count the user accounts: %s", exc)
        return 0


def password_state():
    """Whether the signed-in student still has the password they started with."""
    student_number = current_student_number()
    users = get_users()
    if not student_number or users is None:
        return "—"

    try:
        record = users.get(student_number)
    except Exception:
        return "—"

    if not record:
        return "—"
    return (
        "Still the default one"
        if record["using_default_pw"]
        else "Changed from the default"
    )


def sign_in(username, password):
    """Checks the credentials and opens a session.
    Returns (student_number, None) on success, (None, error_message) on failure."""
    student_number = clean_student_number(username)
    if not student_number:
        return None, "Enter your student number."

    users = get_users()
    if users is None:
        return None, "The user database is unavailable. Try again shortly."

    try:
        record = users.verify(student_number, password)
    except Exception as exc:
        log.error("Sign-in failed for %s: %s", student_number, exc)
        return None, "Could not verify that password. Try again shortly."

    if not record:
        return None, "That student number and password do not match."

    users.record_login(student_number)
    session[SESSION_STUDENT_NUMBER] = record["student_number"]
    session[SESSION_FULL_NAME] = record["full_name"]
    return record["student_number"], None


def sign_out():
    try:
        session.pop(SESSION_STUDENT_NUMBER, None)
        session.pop(SESSION_FULL_NAME, None)
    except RuntimeError:
        pass


def change_password(current_password, new_password, confirm_password):
    """Changes the signed-in student's password.
    Returns (True, None) on success, (False, error_message) on failure."""
    student_number = current_student_number()
    if not student_number:
        return False, "You are not signed in."

    if len(str(new_password or "")) < MIN_PASSWORD_LENGTH:
        return False, f"Use at least {MIN_PASSWORD_LENGTH} characters."
    if new_password != confirm_password:
        return False, "The two new passwords do not match."
    if new_password == current_password:
        return False, "The new password is the same as the current one."

    users = get_users()
    if users is None:
        return False, "The user database is unavailable. Try again shortly."

    try:
        if not users.verify(student_number, current_password):
            return False, "Your current password is not correct."
        users.set_password(student_number, new_password, still_default=False)
    except Exception as exc:
        log.error("Password change failed for %s: %s", student_number, exc)
        return False, "Could not save the new password. Try again shortly."

    return True, None