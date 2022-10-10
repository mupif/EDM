# MongoDB installation

```
wget -qO - https://www.mongodb.org/static/pgp/server-5.0.asc | sudo apt-key add - && echo "deb http://repo.mongodb.org/apt/debian buster/mongodb-org/5.0 main" | sudo tee /etc/apt/sources.list.d/mongodb-org-5.0.list
sudo apt-get update
sudo apt-get -y install mongodb-org
```

# TODO

Enhance `HeavyStruct` to support
* `link` node; optionally specify shape (1 by default, returns scalar from the linked dataset);

   * use `name` to access (ro?) the linked item / list of items
   * use `name_raw` to access (rw) the index / index list itself
