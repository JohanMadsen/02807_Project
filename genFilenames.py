import os.path

n = 100000 # This many files
path_name = '/data/text/'
filesPerDir = 100

def dirnameFun(dir_index,path_name):
    char1 = dir_index % 26
    char2 = dir_index // 26 % 26
    return os.path.join(path_name, '%c%c' % (ord('A') + char2, ord('A') + char1))

def filepath(dirname, file_index):
    return '%s/wiki_%02d' % (dirname, file_index)

def genFilenames(n,filesPerDir, path_name):
    # Given number of files print a list of filenames
    dir_index = -1
    file_index = -1

    # Open file
    file = open("listOfFiles.txt", "w")

    for i in range(n):
        file_index = (file_index + 1) % filesPerDir

        if file_index == 0:
            dir_index += 1

        dirname = dirnameFun(dir_index, path_name)

        file.write(filepath(dirname, file_index) +'\n')
    file.close()

genFilenames(n,filesPerDir,path_name)
