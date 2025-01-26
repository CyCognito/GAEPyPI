import io
import os

import requests
from flask import Flask, request, abort, send_file, Response
from google.cloud.exceptions import NotFound

from gaepypi import Package, GCStorage, GAEPyPIError, PackageIndex
from gaepypi._decorators import basic_auth
from google.appengine.api import app_identity, wrap_wsgi_app

app = Flask(__name__)
app.wsgi_app = wrap_wsgi_app(app.wsgi_app)
storage = None
session = requests.session()


def get_storage():
    global storage
    if not storage:
        bucket_name = os.environ.get('BUCKET_NAME') or app_identity.get_default_gcs_bucket_name()
        storage = GCStorage(bucket_name)
    return storage


@app.route("/")
@basic_auth()
def root():
    return '<a href="/packages">packages</a>'


@app.route("/pypi/")
@basic_auth()
def root_pypi():
    return get_storage().to_html(full_index=True)


@app.route("/", methods=['POST'])
@basic_auth(required_roles=['write'])
def root_post():
    name = request.values.get('name')
    version = request.values.get('version')
    action = request.values.get(':action')

    if name and version and len(request.files) and action == 'file_upload':
        try:
            upload = request.files['content']
            filename = upload.filename
            package = Package(get_storage(), name, version)
            package.put_file(filename, upload.stream)
        except GAEPyPIError as e:
            abort(403)
    return ""


@app.route("/packages", methods=['GET'])
@basic_auth()
def packages_get():
    st = get_storage()
    if st.empty():
        return 'Nothing to see here yet, try uploading a package!'
    else:
        return st.to_html(full_index=False)


@app.route("/packages/<package>", methods=['GET'])
@app.route("/packages/<package>/", methods=['GET'])
@basic_auth()
def packages_get_package(package):
    index = PackageIndex(get_storage(), package)
    if index.exists():
        return index.to_html(full_index=False)
    return proxy_to_pypi_org(package)


@app.route("/packages/<package>/<version>", methods=['GET'])
@app.route("/packages/<package>/<version>/", methods=['GET'])
@basic_auth()
def get(package, version):
    package = Package(get_storage(), package, version)
    if package.exists():
        return package.to_html()
    abort(404)


@app.route("/packages/<name>/<version>/<filename>", methods=['GET'])
@basic_auth()
def package_download(name, version, filename):
    try:
        package = Package(get_storage(), name, version)
        gcs_file = package.get_file(filename)
        return send_file(gcs_file, mimetype='application/octet-stream', as_attachment=True, download_name=filename)
    except (NotFound, GAEPyPIError):
        abort(404)


@app.route("/pypi/<path:package>", methods=['GET'])
@basic_auth()
def pypi_package_get(package):
    st = get_storage()
    index = PackageIndex(st, package)
    if not index.empty() and index.exists(st):
        return index.to_html(full_index=True)

    return proxy_to_pypi_org(package)


def proxy_to_pypi_org(package) -> Response:
    proxy_headers = cleanup_headers(request,
                                    ['authorization', 'x-appengine-auth-domain', 'x-appengine-user-is-admin', 'host'])
    resp = session.get(f"https://pypi.org/simple/{package}", headers=proxy_headers, stream=True)

    return Response(response=read_binary_data(resp), status=resp.status_code, headers=resp.headers)


def read_binary_data(resp) -> bytes:
    buffer = io.BytesIO()
    while True:
        chunk = resp.raw.read(16 * 1024)
        if not chunk:
            break
        buffer.write(chunk)
    return buffer.getvalue()


def cleanup_headers(obj, skip_list) -> dict:
    ret_headers = {}
    for k, v in obj.headers.items():
        if k.lower() not in skip_list:
            ret_headers[k] = v
    return ret_headers


if __name__ == "__main__":
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    # Flask's development server will automatically serve static files in
    # the "static" directory. See:
    # http://flask.pocoo.org/docs/1.0/quickstart/#static-files. Once deployed,
    # App Engine itself will serve those files as configured in app.yaml.
    app.run(host="127.0.0.1", port=8080, debug=True)
