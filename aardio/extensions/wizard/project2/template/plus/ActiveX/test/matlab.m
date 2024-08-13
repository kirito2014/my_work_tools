% MATLAB 创建 COM 对象
sample = actxserver('${COLIBRARY_PATH}.${COCLASS_NAME}');

% 调用Add方法
num = sample.Invoke("Add",1,3)

% 显示结果
disp(num);

% 设置属性
sample.SetAttr("key", 'value2')

% 读取属性
sample.GetAttr("key")

% 设置属性
set(sample, 'key', 'value'); 

% 读取属性
value = get(sample, 'key');
