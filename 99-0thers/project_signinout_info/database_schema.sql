-- 考勤记录表示例表结构
CREATE TABLE IF NOT EXISTS attendance_records (
    id INT AUTO_INCREMENT PRIMARY KEY,
    employee_id VARCHAR(20) NOT NULL,
    employee_name VARCHAR(50) NOT NULL,
    department VARCHAR(50) NOT NULL,
    checkin_time DATETIME NOT NULL,
    batch_name VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_checkin_date (DATE(checkin_time)),
    INDEX idx_department (department)
);

-- 部门加班表结构
CREATE TABLE IF NOT EXISTS department_overtime (
    id INT AUTO_INCREMENT PRIMARY KEY,
    department VARCHAR(50) NOT NULL,
    overtime_date DATE NOT NULL,
    overtime_hours DECIMAL(5,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_overtime_date (overtime_date),
    INDEX idx_dept_overtime (department, overtime_date)
);

-- 员工信息表结构
CREATE TABLE IF NOT EXISTS employees (
    id INT AUTO_INCREMENT PRIMARY KEY,
    employee_id VARCHAR(20) NOT NULL UNIQUE,
    employee_name VARCHAR(50) NOT NULL,
    department VARCHAR(50) NOT NULL,
    position VARCHAR(50),
    hire_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_department (department)
);

-- 插入测试数据
INSERT INTO employees (employee_id, employee_name, department, position) VALUES
('EMP001', '张三', '对公组', '经理'),
('EMP002', '李四', '对公组', '专员'),
('EMP003', '王五', '零售组', '专员'),
('EMP004', '赵六', '信贷组', '经理'),
('EMP005', '钱七', '资管组', '专员');

-- 插入测试考勤记录
INSERT INTO attendance_records (employee_id, employee_name, department, checkin_time, batch_name) VALUES
('EMP001', '张三', '对公组', '2023-11-15 08:10:00', '8:15批次'),
('EMP002', '李四', '对公组', '2023-11-15 08:45:00', '8:50批次'),
('EMP003', '王五', '零售组', '2023-11-15 08:12:00', '8:15批次'),
('EMP004', '赵六', '信贷组', '2023-11-15 09:05:00', '8:50批次'),
('EMP005', '钱七', '资管组', '2023-11-15 08:08:00', '8:15批次');

-- 插入测试加班数据
INSERT INTO department_overtime (department, overtime_date, overtime_hours) VALUES
('对公组', '2023-11-15', 3.5),
('零售组', '2023-11-15', 2.0),
('信贷组', '2023-11-15', 1.5),
('资管组', '2023-11-15', 4.0);