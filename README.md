Setup databse tables:

```
# sudo -u postgres psql discourse
discourse=# set role discourse;
discourse=> \i dbsetup.sql
```

Run `./gather_data.py` with appropriate arguments.

Move files and start redir-server.service.
