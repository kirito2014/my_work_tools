// 主应用逻辑
const ResumeGenerator = {
    // 初始化应用
    init() {
        this.setupEventListeners();
        this.initializeBanks();
        this.initializeDepts();
        this.logMessage('简历生成器已启动');
    },

    // 设置事件监听器
    setupEventListeners() {
        // 移动端菜单切换
        document.getElementById('mobile-menu-button').addEventListener('click', () => {
            const mobileMenu = document.getElementById('mobile-menu');
            mobileMenu.classList.toggle('hidden');
        });

        // 生成方式切换
        document.querySelectorAll('input[name="generate-method"]').forEach(radio => {
            radio.addEventListener('change', (e) => {
                const listFrame = document.getElementById('list-generate-frame');
                const customFrame = document.getElementById('custom-generate-frame');
                
                if (e.target.value === 'all') {
                    listFrame.classList.remove('hidden');
                    customFrame.classList.add('hidden');
                } else {
                    listFrame.classList.add('hidden');
                    customFrame.classList.remove('hidden');
                }
            });
        });

        // 折叠/展开人员列表
        document.getElementById('toggle-person-list').addEventListener('click', (e) => {
            const container = document.getElementById('person-list-container');
            const icon = e.currentTarget.querySelector('i');
            const text = e.currentTarget.lastChild;
            
            if (container.classList.contains('max-h-64')) {
                container.classList.remove('max-h-64');
                container.classList.add('max-h-96');
                icon.classList.remove('fa-chevron-up');
                icon.classList.add('fa-chevron-down');
                text.textContent = ' 展开';
            } else {
                container.classList.remove('max-h-96');
                container.classList.add('max-h-64');
                icon.classList.remove('fa-chevron-down');
                icon.classList.add('fa-chevron-up');
                text.textContent = ' 折叠';
            }
        });

        // 特殊更新菜单
        document.getElementById('menu-special-update').addEventListener('click', () => {
            this.showSpecialUpdateDialog();
        });
        document.getElementById('mobile-menu-special-update').addEventListener('click', () => {
            this.showSpecialUpdateDialog();
            document.getElementById('mobile-menu').classList.add('hidden');
        });

        // 简历校验菜单
        document.getElementById('menu-validation').addEventListener('click', () => {
            this.showValidationDialog();
        });
        document.getElementById('mobile-menu-validation').addEventListener('click', () => {
            this.showValidationDialog();
            document.getElementById('mobile-menu').classList.add('hidden');
        });

        // 关闭对话框按钮
        document.getElementById('close-special-update').addEventListener('click', () => {
            this.hideSpecialUpdateDialog();
        });
        document.getElementById('close-preprocess').addEventListener('click', () => {
            this.hidePreprocessDialog();
        });
        document.getElementById('close-validation').addEventListener('click', () => {
            this.hideValidationDialog();
        });

        // 文件选择按钮
        document.getElementById('select-tech-info').addEventListener('click', () => {
            this.simulateFileSelect('tech-info-path', 'Excel文件 (*.xlsx;*.xls)');
        });
        document.getElementById('select-resume-folder').addEventListener('click', () => {
            this.simulateFolderSelect('resume-folder-path');
        });
        document.getElementById('select-list-file').addEventListener('click', () => {
            this.simulateFileSelect('list-file-path', 'Excel文件 (*.xlsx;*.xls)');
        });

        // 特殊更新对话框文件选择
        document.getElementById('select-special-resume-folder').addEventListener('click', () => {
            this.simulateFolderSelect('special-resume-folder');
        });
        document.getElementById('select-special-info-file').addEventListener('click', () => {
            this.simulateFileSelect('special-info-file', 'Excel文件 (*.xlsx;*.xls)');
        });

        // 校验对话框文件选择
        document.getElementById('select-validation-folder').addEventListener('click', () => {
            this.simulateFolderSelect('validation-folder');
        });

        // 解析按钮
        document.getElementById('parse-tech-info').addEventListener('click', () => {
            this.parseTechInfo();
        });
        document.getElementById('start-parse').addEventListener('click', () => {
            this.startParse();
        });

        // 生成简历按钮
        document.getElementById('generate-resumes').addEventListener('click', () => {
            this.generateResumes();
        });

        // 人员列表控制按钮
        document.getElementById('update-person-list').addEventListener('click', () => {
            this.updatePersonList();
        });
        document.getElementById('confirm-selection').addEventListener('click', () => {
            this.confirmSelection();
        });
        document.getElementById('clear-selection').addEventListener('click', () => {
            this.clearSelection();
        });
        document.getElementById('load-last-selected').addEventListener('click', () => {
            this.loadLastSelection();
        });

        // 部门筛选按钮
        document.getElementById('filter-by-dept').addEventListener('click', () => {
            this.filterByDepartment();
        });
        document.getElementById('clear-filters').addEventListener('click', () => {
            this.clearFilters();
        });

        // 搜索框
        document.getElementById('search-person').addEventListener('input', (e) => {
            this.searchPersons(e.target.value);
        });

        // 特殊更新按钮
        document.getElementById('execute-special-update').addEventListener('click', () => {
            this.executeSpecialUpdate();
        });

        // 简历校验按钮
        document.getElementById('execute-validation').addEventListener('click', () => {
            this.executeValidation();
        });

        // 银行选择变更
        document.getElementById('bank-select').addEventListener('change', (e) => {
            this.updateBankLogo(e.target.value);
        });

        // 点击外部关闭对话框
        document.querySelectorAll('.fixed.inset-0').forEach(dialog => {
            dialog.addEventListener('click', (e) => {
                if (e.target === dialog) {
                    // 根据对话框ID隐藏对应对话框
                    if (dialog.id === 'special-update-dialog') this.hideSpecialUpdateDialog();
                    if (dialog.id === 'preprocess-dialog') this.hidePreprocessDialog();
                    if (dialog.id === 'validation-dialog') this.hideValidationDialog();
                }
            });
        });
    },

    // 模拟文件选择
    simulateFileSelect(inputId, filter = '*') {
        const input = document.getElementById(inputId);
        // 在实际应用中，这里会触发文件选择对话框
        // 现在我们模拟选择文件
        const mockPath = `C:\\Users\\User\\Documents\\sample_${inputId.split('-')[0]}.xlsx`;
        input.value = mockPath;
        this.logMessage(`已选择文件: ${mockPath}`);
    },

    // 模拟文件夹选择
    simulateFolderSelect(inputId) {
        const input = document.getElementById(inputId);
        // 在实际应用中，这里会触发文件夹选择对话框
        // 现在我们模拟选择文件夹
        const mockPath = `C:\\Users\\User\\Documents\\${inputId.split('-')[0]}_folder`;
        input.value = mockPath;
        this.logMessage(`已选择文件夹: ${mockPath}`);
    },

    // 初始化银行列表
    initializeBanks() {
        const bankSelect = document.getElementById('bank-select');
        const banks = [
            { id: '', name: '请选择银行' },
            { id: 'icbc', name: '中国工商银行' },
            { id: 'ccb', name: '中国建设银行' },
            { id: 'abc', name: '中国农业银行' },
            { id: 'boc', name: '中国银行' },
            { id: 'cmb', name: '招商银行' },
            { id: 'spdb', name: '浦发银行' },
            { id: 'cmbc', name: '中国民生银行' },
            { id: 'cib', name: '兴业银行' },
            { id: 'citic', name: '中信银行' }
        ];

        banks.forEach(bank => {
            const option = document.createElement('option');
            option.value = bank.id;
            option.textContent = bank.name;
            bankSelect.appendChild(option);
        });
    },

    // 初始化部门列表
    initializeDepts() {
        const level1Dept = document.getElementById('level1-dept');
        const level2Dept = document.getElementById('level2-dept');

        const level1Options = [
            { id: 'tech', name: '技术部' },
            { id: 'product', name: '产品部' },
            { id: 'operation', name: '运营部' },
            { id: 'hr', name: '人力资源部' }
        ];

        level1Options.forEach(dept => {
            const option = document.createElement('option');
            option.value = dept.id;
            option.textContent = dept.name;
            level1Dept.appendChild(option);
        });

        // 模拟二级部门联动
        level1Dept.addEventListener('change', (e) => {
            level2Dept.innerHTML = '<option value="">全部</option>';
            let level2Options = [];

            switch (e.target.value) {
                case 'tech':
                    level2Options = [
                        { id: 'frontend', name: '前端开发组' },
                        { id: 'backend', name: '后端开发组' },
                        { id: 'test', name: '测试组' },
                        { id: 'devops', name: '运维组' }
                    ];
                    break;
                case 'product':
                    level2Options = [
                        { id: 'requirements', name: '需求分析组' },
                        { id: 'design', name: '设计组' }
                    ];
                    break;
                case 'operation':
                    level2Options = [
                        { id: 'market', name: '市场组' },
                        { id: 'customer', name: '客户组' }
                    ];
                    break;
                case 'hr':
                    level2Options = [
                        { id: 'recruit', name: '招聘组' },
                        { id: 'training', name: '培训组' }
                    ];
                    break;
            }

            level2Options.forEach(dept => {
                const option = document.createElement('option');
                option.value = dept.id;
                option.textContent = dept.name;
                level2Dept.appendChild(option);
            });
        });
    },

    // 更新银行logo
    updateBankLogo(bankId) {
        const bankLogo = document.getElementById('bank-logo');
        if (!bankId) {
            bankLogo.innerHTML = '<i class="fas fa-university text-gray-300 text-3xl"></i>';
            return;
        }

        // 这里可以根据bankId更新logo
        // 现在我们使用Font Awesome图标作为示例
        bankLogo.innerHTML = `<i class="fas fa-building text-primary text-3xl"></i>`;
    },

    // 更新人员列表
    updatePersonList() {
        this.logMessage('正在更新人员名单...');
        const personListBody = document.getElementById('person-list-body');
        personListBody.innerHTML = '';

        // 模拟人员数据
        const mockPersons = [
            { id: '001', name: '张三', dept: '技术部-前端开发组' },
            { id: '002', name: '李四', dept: '技术部-后端开发组' },
            { id: '003', name: '王五', dept: '技术部-测试组' },
            { id: '004', name: '赵六', dept: '产品部-需求分析组' },
            { id: '005', name: '孙七', dept: '产品部-设计组' },
            { id: '006', name: '周八', dept: '运营部-市场组' },
            { id: '007', name: '吴九', dept: '运营部-客户组' },
            { id: '008', name: '郑十', dept: '人力资源部-招聘组' }
        ];

        mockPersons.forEach(person => {
            const row = document.createElement('tr');
            row.className = 'treeview-row';
            row.innerHTML = `
                <td class="px-4 py-2">
                    <input type="checkbox" class="person-checkbox" data-id="${person.id}">
                </td>
                <td class="px-4 py-2 text-sm">${person.id}</td>
                <td class="px-4 py-2 text-sm">${person.name}</td>
                <td class="px-4 py-2 text-sm">${person.dept}</td>
            `;
            personListBody.appendChild(row);
        });

        this.logMessage(`人员名单更新完成，共${mockPersons.length}人`);
    },

    // 搜索人员
    searchPersons(keyword) {
        const rows = document.querySelectorAll('#person-list-body tr');
        keyword = keyword.toLowerCase();

        rows.forEach(row => {
            const id = row.querySelector('td:nth-child(2)').textContent.toLowerCase();
            const name = row.querySelector('td:nth-child(3)').textContent.toLowerCase();
            const dept = row.querySelector('td:nth-child(4)').textContent.toLowerCase();

            if (id.includes(keyword) || name.includes(keyword) || dept.includes(keyword)) {
                row.style.display = '';
            } else {
                row.style.display = 'none';
            }
        });
    },

    // 按部门筛选
    filterByDepartment() {
        const level1 = document.getElementById('level1-dept').value;
        const level2 = document.getElementById('level2-dept').value;
        const rows = document.querySelectorAll('#person-list-body tr');

        rows.forEach(row => {
            const dept = row.querySelector('td:nth-child(4)').textContent;
            let showRow = true;

            if (level1) {
                const level1Name = document.getElementById('level1-dept').options[document.getElementById('level1-dept').selectedIndex].text;
                showRow = showRow && dept.includes(level1Name);
            }

            if (level2) {
                const level2Name = document.getElementById('level2-dept').options[document.getElementById('level2-dept').selectedIndex].text;
                showRow = showRow && dept.includes(level2Name);
            }

            row.style.display = showRow ? '' : 'none';
        });

        this.logMessage(`已按部门筛选人员列表`);
    },

    // 清除筛选
    clearFilters() {
        document.getElementById('level1-dept').value = '';
        document.getElementById('level2-dept').innerHTML = '<option value="">全部</option>';
        const rows = document.querySelectorAll('#person-list-body tr');
        rows.forEach(row => {
            row.style.display = '';
        });
        this.logMessage('已清除所有筛选条件');
    },

    // 确认选择
    confirmSelection() {
        const selectedCheckboxes = document.querySelectorAll('.person-checkbox:checked');
        const selectedCount = selectedCheckboxes.length;
        
        if (selectedCount > 0) {
            const selectedIds = Array.from(selectedCheckboxes).map(checkbox => checkbox.dataset.id);
            this.logMessage(`已选择${selectedCount}人，员工编号: ${selectedIds.join(', ')}`);
            document.getElementById('selected-count').textContent = `已选择 ${selectedCount} 人`;
        } else {
            this.logMessage('未选择任何人员');
            document.getElementById('selected-count').textContent = '未选择任何人员';
        }
    },

    // 清除选择
    clearSelection() {
        document.querySelectorAll('.person-checkbox').forEach(checkbox => {
            checkbox.checked = false;
        });
        this.logMessage('已清除所有选择');
        document.getElementById('selected-count').textContent = '未选择任何人员';
    },

    // 加载上次选择
    loadLastSelection() {
        // 模拟加载上次选择
        this.logMessage('正在加载上次选择...');
        this.clearSelection();
        
        // 模拟选中部分人员
        const lastSelected = ['001', '003', '005'];
        lastSelected.forEach(id => {
            const checkbox = document.querySelector(`.person-checkbox[data-id="${id}"]`);
            if (checkbox) checkbox.checked = true;
        });
        
        this.logMessage(`已加载上次选择，共${lastSelected.length}人`);
        document.getElementById('selected-count').textContent = `已选择 ${lastSelected.length} 人`;
    },

    // 解析技术人员信息
    parseTechInfo() {
        const filePath = document.getElementById('tech-info-path').value;
        if (!filePath) {
            this.logMessage('请先选择技术人员信息文件', 'error');
            return;
        }

        this.logMessage(`开始解析技术人员信息: ${filePath}`);
        
        // 模拟解析过程
        this.updateProgress(0);
        
        setTimeout(() => {
            this.updateProgress(30);
            this.logMessage('正在读取Excel文件...');
        }, 500);
        
        setTimeout(() => {
            this.updateProgress(60);
            this.logMessage('正在解析数据...');
        }, 1000);
        
        setTimeout(() => {
            this.updateProgress(100);
            this.logMessage('技术人员信息解析完成', 'success');
        }, 1500);
    },

    // 开始解析简历
    startParse() {
        const folderPath = document.getElementById('resume-folder-path').value;
        if (!folderPath) {
            this.logMessage('请先选择简历文件夹', 'error');
            return;
        }

        this.logMessage(`开始解析简历文件: ${folderPath}`);
        
        // 模拟解析过程
        this.updateProgress(0);
        
        setTimeout(() => {
            this.updateProgress(20);
            this.logMessage('正在扫描文件夹...');
        }, 500);
        
        setTimeout(() => {
            this.updateProgress(50);
            this.logMessage('正在解析简历文件...');
        }, 1500);
        
        setTimeout(() => {
            this.updateProgress(80);
            this.logMessage('正在写入数据库...');
        }, 2500);
        
        setTimeout(() => {
            this.updateProgress(100);
            this.logMessage('简历解析入库完成', 'success');
        }, 3500);
    },

    // 生成简历
    generateResumes() {
        const bankId = document.getElementById('bank-select').value;
        if (!bankId) {
            this.logMessage('请先选择银行', 'error');
            return;
        }

        const generateMethod = document.querySelector('input[name="generate-method"]:checked').value;
        let targetPersons = '';

        if (generateMethod === 'all') {
            const listFilePath = document.getElementById('list-file-path').value;
            if (!listFilePath) {
                this.logMessage('请先选择名单文件', 'error');
                return;
            }
            targetPersons = `名单文件: ${listFilePath}`;
        } else {
            const selectedCount = document.querySelectorAll('.person-checkbox:checked').length;
            if (selectedCount === 0) {
                this.logMessage('请先选择人员', 'error');
                return;
            }
            targetPersons = `自定义选择: ${selectedCount}人`;
        }

        const bankName = document.getElementById('bank-select').options[document.getElementById('bank-select').selectedIndex].text;
        this.logMessage(`开始生成简历 - 银行: ${bankName}, 目标: ${targetPersons}`);
        
        // 模拟生成过程
        this.updateProgress(0);
        
        const steps = 10;
        const interval = setInterval(() => {
            const currentProgress = parseInt(document.getElementById('progress-bar').style.width);
            const newProgress = Math.min(currentProgress + 10, 100);
            
            this.updateProgress(newProgress);
            
            if (newProgress >= 100) {
                clearInterval(interval);
                this.logMessage('简历生成完成', 'success');
            } else {
                this.logMessage(`正在生成简历... ${newProgress}%`);
            }
        }, 300);
    },

    // 执行特殊更新
    executeSpecialUpdate() {
        const resumeFolder = document.getElementById('special-resume-folder').value;
        const infoFile = document.getElementById('special-info-file').value;
        const forceUpdate = document.getElementById('special-force-update').checked;

        if (!resumeFolder || !infoFile) {
            this.logMessage('请完整填写所有信息', 'error');
            return;
        }

        this.logMessage(`开始执行特殊更新 - 简历文件夹: ${resumeFolder}, 信息文件: ${infoFile}`);
        if (forceUpdate) {
            this.logMessage('强制更新模式已启用');
        }

        // 模拟更新过程
        const logElement = document.getElementById('special-update-log');
        logElement.textContent = '[系统] 开始执行特殊更新...';
        
        setTimeout(() => {
            logElement.textContent += '\n[进度] 读取人员信息...';
        }, 500);
        
        setTimeout(() => {
            logElement.textContent += '\n[进度] 处理简历文件...';
        }, 1500);
        
        setTimeout(() => {
            logElement.textContent += '\n[进度] 更新信息...';
        }, 2500);
        
        setTimeout(() => {
            logElement.textContent += '\n[完成] 特殊更新执行成功';
        }, 3500);
    },

    // 执行简历校验
    executeValidation() {
        const folderPath = document.getElementById('validation-folder').value;
        if (!folderPath) {
            this.logMessage('请先选择简历文件夹', 'error');
            return;
        }

        this.logMessage(`开始执行简历校验: ${folderPath}`);
        
        // 模拟校验过程
        const logElement = document.getElementById('validation-log');
        logElement.textContent = '[系统] 开始执行简历校验...';
        
        setTimeout(() => {
            logElement.textContent += '\n[进度] 扫描简历文件...';
        }, 500);
        
        setTimeout(() => {
            logElement.textContent += '\n[进度] 校验格式...';
        }, 1500);
        
        setTimeout(() => {
            logElement.textContent += '\n[进度] 校验内容完整性...';
        }, 2500);
        
        setTimeout(() => {
            logElement.textContent += '\n[完成] 简历校验完成，共校验10个文件，发现2个警告';
        }, 3500);
    },

    // 更新进度条
    updateProgress(percentage) {
        const progressBar = document.getElementById('progress-bar');
        const progressText = document.getElementById('progress-text');
        
        progressBar.style.width = `${percentage}%`;
        progressText.textContent = `${percentage}%`;
    },

    // 显示日志消息
    logMessage(message, type = 'info') {
        const logContent = document.getElementById('log-content');
        const timestamp = new Date().toLocaleTimeString();
        let prefix = '[系统]';
        let textColor = '';

        switch (type) {
            case 'success':
                prefix = '[成功]';
                textColor = 'text-green-600';
                break;
            case 'error':
                prefix = '[错误]';
                textColor = 'text-red-600';
                break;
            case 'warning':
                prefix = '[警告]';
                textColor = 'text-yellow-600';
                break;
            case 'info':
                prefix = '[系统]';
                break;
        }

        const logEntry = `<span class="${textColor}">${timestamp} ${prefix} ${message}</span>`;
        logContent.innerHTML += '\n' + logEntry;
        
        // 滚动到底部
        const logContainer = document.getElementById('log-container');
        logContainer.scrollTop = logContainer.scrollHeight;
    },

    // 显示特殊更新对话框
    showSpecialUpdateDialog() {
        document.getElementById('special-update-dialog').classList.remove('hidden');
    },

    // 隐藏特殊更新对话框
    hideSpecialUpdateDialog() {
        document.getElementById('special-update-dialog').classList.add('hidden');
    },

    // 显示预处理对话框
    showPreprocessDialog() {
        document.getElementById('preprocess-dialog').classList.remove('hidden');
    },

    // 隐藏预处理对话框
    hidePreprocessDialog() {
        document.getElementById('preprocess-dialog').classList.add('hidden');
    },

    // 显示简历校验对话框
    showValidationDialog() {
        document.getElementById('validation-dialog').classList.remove('hidden');
    },

    // 隐藏简历校验对话框
    hideValidationDialog() {
        document.getElementById('validation-dialog').classList.add('hidden');
    }
};

// 页面加载完成后初始化应用
document.addEventListener('DOMContentLoaded', () => {
    ResumeGenerator.init();
});