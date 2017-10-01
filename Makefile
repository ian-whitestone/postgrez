sphinx:
	cd docs && \
	make html

gh-pages:
	git checkout gh-pages && \
	cp -r docs/build/html/* . && \
	git add -A . && \
	git commit -m "Updated sphinx documentation from master: docs/build/html"
