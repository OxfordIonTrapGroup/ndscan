.. _coding-conventions:

Coding conventions
==================

 - Format *all* code using `YAPF <https://github.com/google/yapf>`_, and make sure
   ``flake8 ndscan`` passes. Configuration files for both are included, and should be
   picked up automatically by either tool. It is just not worth spending time on
   obsessing or arguing about formatting details. (Also, you won't get past the
   automated CI checks if you don't follow those styles.)

 - Unit tests are run using ``python -m unittest -v discover test``; they are
   similarly required to pass in CI before a commit is merged.

 - Documentation is built using Sphinx, and `automatically deployed to GitHub Pages
   <https://oxfordiontrapgroup.github.io/ndscan/>`_. Use the Sphinx docstring format
   throughout (e.g. ``:param foo: Description for the "foo" parameter.``), with regular
   Python annotations to document typing information. To locally check the finished HTML
   output, run ``make html`` in the ``docs/`` directory and open
   ``docs/_build/html/index.html`` in a browser.


Naming
------

 - Underscores mean "package-private", as in not to be fiddled with by users, but
   possibly accessed by other parts of ndscan. (This should likely change, towards
   actually meaning private and public, as the library slowly moves from an
   intentionally restrictive MVP towards a more flexible toolkit.)

 - ``describe()`` generally refers to producing JSON-compatible "stringly-typed" 
   dictionary representations of various bits of the scan schema.
