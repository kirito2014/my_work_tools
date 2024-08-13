import React from 'react'; 
import { useState, useEffect, useCallback } from "react";
import './App.css';

function App(props) {
  const [value, setValue] = useState(0);
  const increment = useCallback(() => {
    setValue(value + 1); 
  }, [value]);

  // https://preactjs.com/guide/v10/hooks/#useeffect
  const [time, setTime] = useState(Date.now());
  useEffect(() => {
    let timer = setInterval(() => {
      setTime(Date.now());
    }, 1000);

    return function cleanup() {
      clearInterval(timer);
    };
  }, []);

  return (
    <div>
      <h1>Hello {this.props.name}!</h1> 
      <h2>
        当前时间:<span>{new Date(time).toLocaleTimeString()}</span>
      </h2>
      当前计数: {value}
      <br />
      <button onClick={increment}>
        点这里增加计数，体验 React Hooks
      </button>
      <br /><br />
      <button onClick={()=>{external.hello("NervJS",setValue)}}>
        点这里直接调用 aardio 函数，然后在 aardio 里回调 React hook 函数
      </button>
      <br /><br />
使用 NervJS  Hooks 就可以 在 web.form 里使用 React Hooks 写网页了。<br />
可完美支持 web.kit( WebKit 内核 ） 扩展库。<br />
      <br />
React Hooks 简洁优雅, 大幅降低了前端开发的门槛和学习成本。<br />
只要熟悉 useState, useEffect, useCallback, useContext 等几个非常简单的 Hooks 的用法，<br />
你就可以运用自如，强烈推荐大家学习和使用。<br /><br />
    </div>
  );
}

export default App;
