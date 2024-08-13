;; AutoCAD（AutoLisp） 调用 aardio 示例。 

;; 方式1，推荐，写起来简单
(defun GetFilesByInvoke (filter title / obj files)
  ;; 获取或创建 COM 对象
  (setq obj (vlax-get-or-create-object "${COLIBRARY_PATH}.${COCLASS_NAME}"))

  ;; 获取 AutoCAD 窗口句柄，设为对话框所有窗口
  (setq hwndOwner (itoa(vla-get-hwnd(vlax-get-acad-object))))
  
  ;; AutoLisp 符号名自动转大写，在 aardio 里要实现为大写的 GETFILES
  ;; vlax-invoke 会将 aardio 返回值自动转为 LISP 类型，比 vlax-invoke-method 简单。
  (setq files (vlax-invoke obj 'GetFiles filter title hwndOwner))
  
  ;; 释放 COM 对象
  (vlax-release-object obj)  

  files 
)

;; 方式2，不推荐，写起来比较麻烦
(defun GetFilesByInvokeMethod ( filter title / obj var arr files)
  ;; 获取或创建 COM 对象
  (setq obj (vlax-get-or-create-object "${COLIBRARY_PATH}.${COCLASS_NAME}"))

  ;; 获取 AutoCAD 窗口句柄，设为对话框所有窗口
  (setq hwndOwner (itoa(vla-get-hwnd(vlax-get-acad-object)))) 
   
  ;; vlax-invoke-method 只能调用 ODL 声明的函数（但不需要理会 LISP 符号名大写问题）。
  ;; 如果不想改 ODL ，可以使用 aardio.idl 声明的 Invoke 函数间接调用其他函数（第一个参数为函数名）。
  (setq var (vlax-invoke-method obj 'Invoke "GETFILES"  filter title hwndOwner))

  ;; vlax-invoke-method 返回的 Variant 类型要自己转换
  (setq arr (vlax-variant-value var) )

  ; 检查是否为 SAFEARRAY 类型（这也是文档里不写的函数，safearray-value的好处是不会报错，可以用来检查）
  (if (safearray-value arr)
    (progn
      ; 将 SAFEARRAY 转换为 Lisp 列表
      (setq files(vlax-safearray->list arr)) 
    ) 
  )
 
  ;; 释放 COM 对象
  (vlax-release-object obj)
  files
)

(setq files ( GetFilesByInvoke  "所有文件|*.*|" "标题：AutoCAD") )

;; 检查返回值并打印结果
(if files
(progn
	(princ "\nSelected files:")
	(mapcar 'print files)
)
(princ "\nNo files selected or operation canceled.")
)

(princ)