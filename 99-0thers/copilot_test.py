#创建一个冒泡排序的函数，以及一个排序的测试函数
def bubble_sort(alist):
    for passnum in range(len(alist)-1,0,-1):
        for i in range(passnum):
            if alist[i]>alist[i+1]:
                temp = alist[i]
                alist[i] = alist[i+1]
                alist[i+1] = temp

#测试函数
def test_sort():
    alist = [54,26,93,17,77,31,44,55,20]
    bubble_sort(alist)
    print(alist)
    


