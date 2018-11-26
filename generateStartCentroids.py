n=1000
file = open("Centroid.txt", "w")
for i in range(n):
    s=""
    s+=str(i/n)
    s+=","
    s+=str(i/n)
    s+=","
    s+=str(1-i/n)
    s+=","
    s+=str(i/n)
    s+="\n"
    file.write(s)