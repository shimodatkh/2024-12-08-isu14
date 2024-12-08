sudo journalctl --rotate
sudo journalctl --vacuum-time=1s
# go build -o isuride
sudo systemctl restart isuride-go.service
# sudo systemctl status isuride-go.service