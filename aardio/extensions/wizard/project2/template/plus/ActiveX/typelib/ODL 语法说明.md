# ODL �﷨�����ĵ�

>�� aardio ��һ�㲻��Ҫ�Ķ� *.odl ���Ϳ������ļ��� ����Ĭ�ϾͿ��ԣ����нӿں������� com.activeX �Զ�������
>
>���Ҫ�Զ������Ϳ⣬����̳� aardio.idl �ṩ�� IDispeatchExecutable �ӿڡ� ���� DISPID ��Ӧ���� 10000 ��aardio �Զ����� DISPID �� 10000 Ϊ��ʼֵ��������
>
>���� aardio ����󡢺�������ʵ���� IDispatch �ӿڣ� �������� DISPID_NEWENUM ����ʱ���� IEnumVARIANT ö�ٽӿڡ�


## һ���������Ϳ⣨library��

### 1.1 �﷨
```odl
[attributes]
library ���Ϳ��� { 
    importlib("����");
    [attributes]
    interface/interface_name : IDispatch { 
        ��������;
    };
};
```

### 1.2 �﷨Ԫ��˵��
- `attributes`: ���԰��� `helpstring`, `helpcontext`, `lcid`, `restricted`, `hidden`, `control`, `uuid`, `version` ���ԣ����� `uuid` �Ǳ���ġ�
- `library`: ���Ϳ�����֡�
- `importlib`: �����׼�⡣
- `interface`: �ӿڶ��塣

### 1.3 ��ע
- `library` ���ʽ����������κ����Ͷ���֮ǰ��

### 1.4 ʾ��
```odl
import "aardio.idl";

[
uuid(27A24EA2-F236-4FE4-A918-44AAB7A8DC5C),
version(1.0)
]
library aardioTestControl {

	importlib("stdole32.tlb");    
 
	[ uuid(EC32DF0E-0947-4DF1-827D-7073D376995D),control ]
	coclass Sample {
		
		//Ĭ�Ͻӿ�
		[default] dispinterface IDispatchExecutable;
		
		//Ĭ���¼�Դ�ӿ�
		[default,source] dispinterface IDispatchExecutableEvent;
	}; 
};
```

## ���������Զ����ӿڣ�dispinterface��

### 2.1 �﷨
```odl
[attributes]
dispinterface �ӿ��� { 
    �����б� 
};
```

### 2.2 �﷨Ԫ��˵��
- `attributes`:  һ��ֻҪָ�� `uuid` �Ϳ����ˡ�
- `functionlist`: �ӿ���ÿ��������ԭ���б�

### 2.3 ��������
```odl
[attributes] returntype [calling convention] funcname(params);
```
- `attributes`: �ɰ��� `helpstring`, `helpcontext`, `string`, `propget`, `propput`, `propputref`, `bindable`, `defaultbind`, `displaybind`, `vararg` �ȡ�
- `params`: �����б����԰��� `in`, `out`, `optional`, `string` ���ԡ�

### 2.4 ��ע
- aardio ����õ��� dispinterface �ӿڣ���������Ǽ̳��� IDispatch ���Զ����ӿڡ�
- dispinterface �ӿڲ��ܼ̳��������ӿڣ���Ϊ����̳���IDispatch����
- dispinterface ����ҪҪ�����һ������ָ������ֵ������Ҳ����Ҫ����  `HRESULT` ֵ������ֱ��ָ��ʵ����Ҫ�ķ���ֵ�Ϳ����ˡ�
- ��� [out] ��ǲ������������������ʵ���� COM �ӿں������������������ֱ��ʹ�÷���ֵ���򵥡�
- ���� DISPID ��ֵ����С�� 10000 �����ڵ��� 0 ������ 10000 �� DISPID �� aardio �Զ����䡣

### 2.5 ʾ��
```odl
	[ uuid(1C8736BC-8C0C-4DB6-9FAD-1C6A0CDF1FA2) ]
	dispinterface  IDispatchSample{ 
		properties:
		methods:  
			[ id(10) ]
			void Test( [in] BSTR str,[in,out] VARIANT *out1,[in,out] VARIANT *out2);
	} 
```

## ��������ӿڣ�interface��

### 2.1 �﷨
```odl
[attributes]
interface �ӿ��� [: ���ӿ���] { 
    �����б� 
};
```

### 2.2 �﷨Ԫ��˵��
- `attributes`: ���԰��� `dual`, `helpstring`, `helpcontext`, `hidden`, `odl`, `oleautomation`, `uuid`, `version` ���ԣ����� `odl` �� `uuid` �Ǳ���ġ�
- `functionlist`: �ӿ���ÿ��������ԭ���б�

### 2.3 ��������
```odl
[attributes] returntype [calling convention] funcname(params);
```
- `attributes`: �ɰ��� `helpstring`, `helpcontext`, `string`, `propget`, `propput`, `propputref`, `bindable`, `defaultbind`, `displaybind`, `vararg` �ȡ�
- `params`: �����б����԰��� `in`, `out`, `optional`, `string` ���ԡ�

### 2.4 ��ע
- �ӿ���ĺ������� `HRESULT` ֵ����ʵ����ֵָ��Ϊ���ز�����ʼ�������һ��������
- ˫�ؽӿڱ���� `IDispatch` �̳С�

### 2.5 ʾ��
```odl
[uuid(BFB73347-822A-1068-8849-00DD011087E8), version(1.0)]
interface Hello : IUnknown { 
    void HelloProc([in, string] unsigned char* pszString); 
    void Shutdown(void); 
};

[dual]
interface IMyInt : IDispatch { 
    [propget] HRESULT MyMessage([in, lcid] LCID lcid, [out, retval] BSTR* pbstrRetVal); 
    [propput] HRESULT MyMessage([in] BSTR rhs, [in, lcid] DWORD lcid);
    HRESULT SayMessage([in] long NumTimes, [in, lcid] DWORD lcid, [out, retval] BSTR* pbstrRetVal); 
}
```


## �ġ���������ࣨcoclass��

### 3.1 �﷨
```odl
[attributes]
coclass ���� { 
    [attributes2] [interface | dispinterface] �ӿ���; 
};
```

### 3.2 �﷨Ԫ��˵��
- `attributes`: `uuid` �����Ǳ���ģ��������԰��� `helpstring`, `helpcontext`, `version`, `licensed`, `control`, `hidden`, `appobject` �ȡ�
- `attributes2`: `interface` �� `dispinterface` �Ŀ�ѡ���ԣ����� `source`, `default`, `restricted` �ȡ�
- `interfacename`: �� `interface` �� `dispinterface` �����Ľӿ�����

### 3.3 ��ע
- `coclass` ����һ������Ϊһ��ʵ�֣����� `QueryInterface` �ڽӿڼ�֮���ѯ��

### 3.4 ʾ��
```odl
[uuid(BFB73347-822A-1068-8849-00DD011087E8), version(1.0), helpstring("A class"), helpcontext(2481), appobject]
coclass myapp {
    [source] interface IMydocfuncs; 
    dispinterface DMydocfuncs; 
};

[uuid(00000000-0000-0000-0000-123456789019)]
coclass foo {
    [restricted] interface bar;
    interface bar;
}
```

���ĵ������� ODL �﷨�Ĺؼ�Ҫ�أ����ڿ������ź����ջ���д����