
# First time requirements: #
```
pip install twine wheel
```
Create account on pypi and pypitest by going on websites.

Then create a ~/.pypirc file with:
```
[pypi]
repository: https://upload.pypi.org/legacy/
username: YOUR_USERNAME_HERE
password: YOUR_PASSWORD_HERE

[testpypi]
repository: https://test.pypi.org/legacy/
username: YOUR_USERNAME_HERE
password: YOUR_PASSWORD_HERE

```

# Checklist for releasing pypeman on pypi in version x.x.x #

- [ ] Update CHANGELOG.md
- [ ] Update version number (can also be minor or major)
```
vim pypeman/__init__.py # or 'bumpversion patch' if installed
```
- [ ] Install the package again for local development, but with the new version number:
```
python setup.py develop
```
- [ ] Run the tests and fix eventual errors:
```
tox
```
- [ ] Commit the changes:
```
git add CHANGELOG.md
git commit -m "Changelog for upcoming release x.x.x."
```
- [ ] tag the version
```
git tag x.x.x # The same as in pypeman/__init__py file
```
- [ ] push on github
```
git push --tags
```
- [ ] Make a release on github

    - Go to the project homepage on github
    - Near the top, you will see Releases link. Click on it.
    - Click on 'Draft a new release'
    - Fill in all the details
        - Tag version should be the version number of your package release
        - Release Title can be anything you want.
    - Click Publish release at the bottom of the page
    - Now under Releases you can view all of your releases.
    - download the tar file (pypeman-<tag>.tar.gz) and unpack it  (tar xvf pypeman-<tag>.tar.gz)
    - cd pypeman-<tag>

- [ ] Install npm with any of below options if not existing (needed to build web client)
   - use nodeenv (pip install nodeenv ; nodeenv install -p )
   - use nvm
   - install node from the node web page ( https://nodejs.org/en/download/ )

- [ ] Generate webclient:
```
cd pypeman/client
npm install
npm run build
cd ../..
```
If the shell freezes after the message "Build complete." then press ctrl-C and continue the instructions.

- [ ] Generate packages:
```
pip install wheel  # if not already installed in your venv
python setup.py sdist bdist_wheel
```
- [ ] publish release on pypi and see result on https://testpypi.python.org/pypi :
```
pip install twine  # if not installed in your venv
twine upload -r testpypi dist/*
```
- [ ] Then when all is ok, release on PyPI by uploading both sdist and wheel:
```
twine upload dist/*
```

- [ ] Test that it pip installs:
```
mktmpenv
pip install my_project
<try out my_project>
deactivate
```

- [ ] Push: `git push`
- [ ] Push tags: `git push --tags`
- [ ] Check the PyPI listing page to make sure that the README, release notes, and roadmap display properly. If not, copy and paste the RestructuredText into http://rst.ninjs.org/ to find out what broke the formatting.
- [ ] Edit the release on GitHub (e.g. https://github.com/audreyr/cookiecutter/releases). Paste the release notes into the release's release page, and come up with a title for the release.
