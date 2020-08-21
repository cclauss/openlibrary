#!/bin/sh

pytest . \
       --ignore=openlibrary/records/tests \
       --ignore=tests/integration \
       --ignore=scripts/2011 \
       --ignore=infogami \
       --ignore=vendor
RETURN_CODE=$?

flake8 --exit-zero --count --select=E722 --show-source  # Show all the bare exceptions
safety check || true  # Show any insecure dependencies

exit ${RETURN_CODE}
