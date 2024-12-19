# from tqdm.rich import trange
# X=50000
# for i in trange(X):
#     for j in range(i):
#         k = j*i
    # print(
    #     f'|{"♦" * ((i+1)*50 //X):50}|',
    #     f'{(i+1)*100//X}%',
    #     end='\r')

from tqdm import tqdm
import time

bar = tqdm(total=100)
bar.set_description("Processing")
time.sleep(2)
bar.update(25)
bar.set_description("Merging")
time.sleep(2)
bar.update(25)
bar.set_description("updating")
time.sleep(2)
bar.update(25)
bar.set_description("hhhh")
time.sleep(2)
bar.update(25)