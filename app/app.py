from flask import Flask

app = Flask(__name__)



@app.route("/")
def home():
    return "<center><h3>Welcome to hi</h3></center>"


if __name__ == '__main__':
    app.run('0.0.0.0')
