# NOTES

## Python Execution

```bash
source ~/.imap/creds/mattyo161@aol.com.sh
gunzip -c messages-aol-all.jsonl.gz \
| jq -rc 'select(.reply_to[] | test("@cp20.com"))' \
| python ./imap_cli.py "${AOL_CREDS[@]}" --progress --throttle-delay 1 -v fetch \
| gzip > messages-aol-cp20.jsonl.gz
```

```bash
source ~/.imap/creds/matt.ouellette@zoho.com.sh
export ACCOUNT=zoho
export CREDS=("${ZOHO_CREDS[@]}")
python ./imap_cli.py "${CREDS[@]}" --progress -v mailboxes \
| tee mailboxes-${ACCOUNT}.jsonl \
| python ./imap_cli.py "${CREDS[@]}" --progress -v messages \
| tee messages-${ACCOUNT}.jsonl \
| python ./imap_cli.py "${CREDS[@]}" --progress -v fetch \
| gzip > messages-${ACCOUNT}.jsonl.gz
```

```bash
source ~/.imap/creds/mattyo161@gmail.com.sh
export ACCOUNT=gmail
export CREDS=("${GMAIL_CREDS[@]}")
python ./imap_cli.py "${CREDS[@]}" --pool-size 1 -v mailboxes \
| tee mailboxes-${ACCOUNT}.jsonl \
| jq -rc '.match = ([.flags[]|[.]|inside(["\\Noselect","\\All"])] | any) | select(.match|not)' \
| python ./imap_cli.py "${CREDS[@]}" --pool-size 5 -v messages \
| tee messages-${ACCOUNT}.jsonl \
| python ./imap_cli.py "${CREDS[@]}" --progress -v --pool-size 12 fetch \
| gzip > messages-${ACCOUNT}.jsonl.gz
```

```shell
python ./imap_cli.py "${CREDS[@]}" --pool-size 1 -v mailboxes \
| tee mailboxes-${ACCOUNT}.jsonl \
| jq -rc '.match = ([.flags[]|[.]|inside(["\\Noselect","\\All"])] | any) | select(.match|not)' \
| python ./imap_cli.py "${CREDS[@]}" --pool-size 5 -v messages \
> messages-${ACCOUNT}.jsonl
```

Resume:
```shell
cat messages-${ACCOUNT}-all-v2.jsonl \
| python ./imap_cli.py "${CREDS[@]}" --throttle-delay 1 --progress -v --pool-size 12 fetch \
| gzip > messages-${ACCOUNT}.jsonl.gz
```