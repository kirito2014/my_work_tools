import random
 
# 示例数据
participants = [
    {"Name": "王穆军", "Seeded": False},
    {"Name": "巫礼祥", "Seeded": False},
    {"Name": "汪进"  , "Seeded": False},
    {"Name": "邹容臻", "Seeded": False},
    {"Name": "袁思程", "Seeded": False},
    {"Name": "郭超"  , "Seeded": False},
    {"Name": "康利"  , "Seeded": True},
    {"Name": "彭祺"  , "Seeded": False},
    {"Name": "孙贤峰", "Seeded": False},
    {"Name": "曾宁"  , "Seeded": True},
    {"Name": "陈雯雯", "Seeded": False},
    {"Name": "程浪花", "Seeded": False}
]
 
# 按种子标记分组
seeded_players = [p for p in participants if p["Seeded"]]
non_seeded_players = [p for p in participants if not p["Seeded"]]
 
# 随机打乱
random.shuffle(seeded_players)
random.shuffle(non_seeded_players)
 
# 分成两个组表
group_a = []
group_b = []
 
# 将种子选手交替分配到两组
for i, player in enumerate(seeded_players):
    if i % 2 == 0:
        group_a.append(player)
    else:
        group_b.append(player)
 
# 将非种子选手交替分配到两组
for i, player in enumerate(non_seeded_players):
    if i % 2 == 0:
        group_a.append(player)
    else:
        group_b.append(player)
 
# 打印分组结果
print("Group A:", [p["Name"] for p in group_a])
print("Group B:", [p["Name"] for p in group_b])
 
# 创建赛程表
def create_match_schedule(group_a, group_b):
    schedule = []
    random.shuffle(group_a)
    random.shuffle(group_b)
    
    for player_a, player_b in zip(group_a, group_b):
        # 跳过种子对种子组合
        if player_a["Seeded"] and player_b["Seeded"]:
            continue
        match = (player_a["Name"], player_b["Name"])
        schedule.append(match)
    
    return schedule
 
# 生成并打印赛程
schedule = create_match_schedule(group_a, group_b)
print("Match Schedule:")
for match in schedule:
    print(f"{match[0]} vs {match[1]}")