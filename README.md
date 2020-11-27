# MTA

## this should be the root directory the running application

## Installation
All the dependencys are listed in requirements.txt. You can install them by downloading [Anaconda](https://www.anaconda.com/).
For dash-auth package, certain version or above is required.

Caveat: All the python package need to be installed under same virtualenv. For exmaple, dash family need live with panda family.
```sh
(base)pip install dash-auth==1.3.1
```

## Auth in development mode

There are two mode of using this application: development(aka. dev) and production.
By default, we use [HTTP Basic Auth](https://dash.plot.ly/authentication) to manage user login.
Authentication is disabled during dev mode, so that every time when users refresh the page, they won't be bothered to enter credentials.
However, you will be always required to add a file named "config.json" into your "/local" directory, with username and password in "users" property. Below is a sample config.
```js
{
    "users": {
        "Kanye": "000000",
        "jeff": "ilovemywife"
    }
}
```
In order to use dev mode, run your app as follows:
```sh
env=development python app.py
```