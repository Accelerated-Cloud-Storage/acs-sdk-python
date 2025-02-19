ACS Python SDK Documentation

Generating Documentation
~~~~~~~~~~~~~~~~~~~~~~~~
Note: ACS's `requirement-docs.txt <https://github.com/boto/botocore/blob/develop/requirements-docs.txt>`_ must be installed prior to attempting the following steps.

Sphinx is used for documentation. You can generate HTML locally and see it in your browser with the following:

.. code-block:: sh

    $ cd docs
    $ pip install -r requirements-docs.txt
    $ make html
    $ cd /build/html
    $ python -m http.server