# str = "1 2 3 \n" \
#       "4 5 6 \n"
#
# print(str)

header = "label"

for i in range(0, 784) :
    header+=(",pixel"+str(i))
print(header)