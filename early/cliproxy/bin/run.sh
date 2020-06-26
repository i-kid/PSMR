sudo rm -rf cliproxy*
sudo rm -rf *.db
echo "all you want is deleted, please wait...."
sleep 2
bash ./build.sh
echo "no matter you change source code or not, the app has been rebuild"
sleep 2
goreman start