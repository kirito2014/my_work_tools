interface aardioExternal {  
    /** 这是一个 aardio  函数 */
    hello():Promise<string>; 
}

declare var aardio: aardioExternal
 