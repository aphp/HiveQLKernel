# HiveQL Kernel

If you are going to connect using kerberos:

```
sudo apt-get install python3-dev libsasl2-dev libsasl2-2 libsasl2-modules-gssapi-mit
```


To install the kernel:

```
pip install --upgrade hiveqlKernel
jupyter hiveql install --user
```

Run tests:

```
python -m pytest
```

How to use in Notebooks:


First create your connection, inside a Notebook cell configure the connection:

```
$$ url=hive://<kerberos-username>@<hive-host>:<hive-port>/<db-name>
$$ connect_args={"auth": "KERBEROS","kerberos_service_name": "hive"}
$$ pool_size=5
$$ max_overflow=10
```

These args are passed to sqlalchemy, who registered pyHive as the 'hive' SQL back-end.
See [github.com/dropbox/PyHive](https://github.com/dropbox/PyHive/#sqlalchemy).

You can also change the default display limit (=20) like this :

```
$$ default_limit=50
```


You can now type HiveQL inside the next Notebook cells:

```
SHOW TABLES
```

Have fun!
