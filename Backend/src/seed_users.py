"""
seed_users.py

Puts the class list into the dashboard's `users` table so students can sign in.

    python3 seed_users.py                     add missing accounts
    python3 seed_users.py --show              list what is in the table
    python3 seed_users.py --reset-passwords   put everybody back on the default

Reads class_list.csv (student_number, first_names, surname, initials), which is
the COS735 class list exported to CSV so the Pi does not need openpyxl to read
the original .xlsx.

Only rows that do not exist are inserted, so running this again after students
have changed their passwords changes nothing.
"""

import argparse
import csv
import logging
import os

import auth

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("seed_users")

CLASS_LIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), "class_list.csv")


def read_class_list(path=CLASS_LIST):
    students = []
    with open(path, newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            number = auth.clean_student_number(row.get("student_number"))
            if not number:
                continue
            students.append({
                "student_number": number,
                "first_names": (row.get("first_names") or "").strip(),
                "surname": (row.get("surname") or "").strip(),
                "initials": (row.get("initials") or "").strip(),
            })
    return students


def main():
    parser = argparse.ArgumentParser(description="Seed the dashboard users table.")
    parser.add_argument("--class-list", default=CLASS_LIST, help="CSV of the class list")
    parser.add_argument("--show", action="store_true", help="list the accounts and exit")
    parser.add_argument("--reset-passwords", action="store_true",
                        help="overwrite every password with the default one")
    args = parser.parse_args()

    users = auth.get_users()
    if users is None:
        log.error("Could not reach the user database, nothing was changed.")
        return 1

    if args.show:
        for student in read_class_list(args.class_list):
            record = users.get(student["student_number"])
            if not record:
                print(f"{student['student_number']:<10} {record} MISSING")
                continue
            state = "default password" if record["using_default_pw"] else "changed"
            print(f"{record['student_number']:<10} {record['full_name']:<32} {state}")
        return 0

    students = read_class_list(args.class_list)
    if not students:
        log.error("No student numbers found in %s", args.class_list)
        return 1

    result = users.seed(students, reset_passwords=args.reset_passwords)
    log.info("%s: %d account(s) added, %d already existed.",
             args.class_list, result["added"], result["existing"])
    log.info("Username is the student number, password is %s<student_number>%s",
             auth.PASSWORD_PREFIX, auth.PASSWORD_SUFFIX)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())