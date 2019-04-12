# HiveQL Kernel

### Requirements

If you are going to connect using kerberos:

```
sudo apt-get install python3-dev libsasl2-dev libsasl2-2 libsasl2-modules-gssapi-mit
```

### Installation

To install the kernel:

```
pip install --upgrade hiveqlKernel
jupyter hiveql install --user
```

### Connection configuration

Two methods are available to connect to a Hive server:

* Directly inside the notebook
* Using a configuration file

If the configuration file is present, everytime you run a new HiveQL kernel it uses it, else you must configure your connection inside the notebook. The configuration in the notebook overwrites the one in the configuration file if present.

#### Configure directly in the notebook cells

Inside a Notebook cell, copy&paste this, change the configuration to match your needs, and run it.

```
$$ url=hive://<kerberos-username>@<hive-host>:<hive-port>/<db-name>
$$ connect_args={"auth": "KERBEROS", "kerberos_service_name": "hive", "configuration": {"tez.queue.name": "myqueue"}}
$$ pool_size=5
$$ max_overflow=10
```

These args are passed to sqlalchemy, who registered pyHive as the 'hive' SQL back-end.
See [github.com/dropbox/PyHive](https://github.com/dropbox/PyHive/#sqlalchemy).

#### Configure using a configuration file

The HiveQL kernel is looking for the configuration file at `~/.hiveql_kernel.conf` by default. You can specify another path using `HIVE_KERNEL_CONF_FILE`.

The contents must be like this (in json format):

```
{ "url": "hive://<kerberos-username>@<hive-host>:<hive-port>/<db-name>", "connect_args" : { "auth": "KERBEROS", "kerberos_service_name":"hive", "configuration": {"tez.queue.name": "myqueue"}}, "pool_size": 5, "max_overflow": 10, "default_limit": 20, "display_mode": "be" }
```


### Usage

Inside a HiveQL kernel you can type HiveQL directly in the cells and it displays a HTML table with the results.

You also have other options, like changing the default display limit (=20) like this :

```
$$ default_limit=50
```

Some hive functions are extended. They allow to filter with some patterns.

```
SHOW TABLES <pattern>
SHOW DATABASES <pattern>
```


### Run tests

```
python -m pytest
```


Have fun!
