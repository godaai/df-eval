# Build the Doc

## Environment Setup

Install the dependencies listed in `requirements-doc.txt`. This includes tools to build this doc.

Navigate to the project folder and build the project:

```bash
cd docs
sphinx-build -b html ./ ./_build/html
```

Web-related files will be generated in the `docs/_build` directory.

## Start HTTP Server

After building the HTML files, you can use the built-in HTTP Server in Python and open http://127.0.0.1:8000 in your browser to view the result:

```bash
cd _build/html
python -m http.server 8000
```