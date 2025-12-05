// 全局配置对象
let config = {};

// 初始化应用
document.addEventListener('DOMContentLoaded', function() {
    loadConfig();
    setupEventListeners();
});

// 设置事件监听器
function setupEventListeners() {
    // 标签页切换
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            const tabId = this.getAttribute('data-tab');
            switchTab(tabId);
        });
    });

    // 保存配置
    document.getElementById('save-btn').addEventListener('click', saveConfig);

    // 上传配置
    document.getElementById('load-btn').addEventListener('click', function() {
        document.getElementById('config-file').click();
    });

    document.getElementById('config-file').addEventListener('change', uploadConfig);

    // 项目相关
    document.getElementById('add-project').addEventListener('click', openProjectModal);
    document.getElementById('cancel-modal').addEventListener('click', closeProjectModal);
    document.getElementById('project-form').addEventListener('submit', saveProject);

    // 正则表达式相关
    document.getElementById('add-ref-pattern').addEventListener('click', addRefPattern);
    document.getElementById('remove-ref-pattern').addEventListener('click', removeRefPattern);
    document.getElementById('add-cleanup-pattern').addEventListener('click', addCleanupPattern);
    document.getElementById('remove-cleanup-pattern').addEventListener('click', removeCleanupPattern);

    // 过滤规则相关
    document.getElementById('add-exclude-pattern').addEventListener('click', addExcludePattern);
    document.getElementById('remove-exclude-pattern').addEventListener('click', removeExcludePattern);
    document.getElementById('add-include-pattern').addEventListener('click', addIncludePattern);
    document.getElementById('remove-include-pattern').addEventListener('click', removeIncludePattern);
}

// 切换标签页
function switchTab(tabId) {
    // 移除所有激活状态
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.remove('border-primary', 'text-primary');
        btn.classList.add('border-transparent', 'text-gray-500');
    });
    document.querySelectorAll('.tab-content').forEach(content => {
        content.classList.remove('active');
    });

    // 添加当前标签页激活状态
    document.querySelector(`[data-tab="${tabId}"]`).classList.remove('border-transparent', 'text-gray-500');
    document.querySelector(`[data-tab="${tabId}"]`).classList.add('border-primary', 'text-primary');
    document.getElementById(tabId).classList.add('active');
}

// 加载配置
function loadConfig() {
    fetch('/api/config')
        .then(response => response.json())
        .then(data => {
            config = data;
            renderConfig();
        })
        .catch(error => {
            showMessage('加载配置失败: ' + error.message, 'error');
        });
}

// 渲染配置
function renderConfig() {
    // 处理配置
    renderProcessingConfig();
    
    // 项目配置
    renderProjects();
    
    // 文件模板
    renderTemplates();
    
    // 正则表达式
    renderRegex();
    
    // 过滤规则
    renderFilters();
    
    // 输出配置
    renderOutput();
    
    // 进度条配置
    renderProgress();
}

// 渲染处理配置
function renderProcessingConfig() {
    const processing = config.processing || {};
    document.getElementById('remove_suffix').value = processing.remove_suffix || 'Y';
    document.getElementById('suffix_identifier').value = processing.suffix_identifier || '_PC';
    document.getElementById('filter_schema').value = processing.filter_schema || 'AGL';
}

// 渲染项目配置
function renderProjects() {
    const projects = config.projects || {};
    const tbody = document.getElementById('projects-body');
    tbody.innerHTML = '';
    
    for (const [name, project] of Object.entries(projects)) {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td class="px-6 py-4 whitespace-nowrap">${name}</td>
            <td class="px-6 py-4 whitespace-nowrap">${project.prefix || ''}</td>
            <td class="px-6 py-4 whitespace-nowrap">${project.theme || ''}</td>
            <td class="px-6 py-4">${project.description || ''}</td>
            <td class="px-6 py-4 whitespace-nowrap">
                <button onclick="editProject('${name}')" class="text-primary hover:text-primary/80 mr-3">
                    <i class="fa fa-edit"></i>
                </button>
                <button onclick="deleteProject('${name}')" class="text-red-500 hover:text-red-600">
                    <i class="fa fa-trash"></i>
                </button>
            </td>
        `;
        tbody.appendChild(row);
    }
}

// 渲染文件模板
function renderTemplates() {
    const templates = config.file_templates || {};
    const container = document.getElementById('templates-container');
    container.innerHTML = '';
    
    for (const [key, template] of Object.entries(templates)) {
        const templateDiv = document.createElement('div');
        templateDiv.className = 'border rounded-md p-4';
        templateDiv.innerHTML = `
            <h3 class="text-lg font-medium mb-3">${template.name || key}</h3>
            <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                    <label class="block text-sm font-medium text-gray-700 mb-1">模板名称</label>
                    <input type="text" data-key="${key}" data-field="name" value="${template.name || ''}" 
                           class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-primary">
                </div>
                <div>
                    <label class="block text-sm font-medium text-gray-700 mb-1">表名所在行号</label>
                    <input type="number" data-key="${key}" data-field="table_name" value="${template.lines?.table_name || 8}" 
                           class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-primary">
                </div>
                <div>
                    <label class="block text-sm font-medium text-gray-700 mb-1">开发人员行号</label>
                    <input type="number" data-key="${key}" data-field="developer" value="${template.lines?.developer || 14}" 
                           class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-primary">
                </div>
                <div>
                    <label class="block text-sm font-medium text-gray-700 mb-1">分隔符</label>
                    <input type="text" data-key="${key}" data-field="delimiter" value="${template.delimiter || ':'}" 
                           class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-primary">
                </div>
                <div class="md:col-span-2">
                    <label class="block text-sm font-medium text-gray-700 mb-1">文件匹配模式</label>
                    <input type="text" data-key="${key}" data-field="pattern" value="${template.file_pattern || ''}" 
                           class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-primary">
                </div>
            </div>
        `;
        container.appendChild(templateDiv);
    }
}

// 渲染正则表达式
function renderRegex() {
    const regex = config.regex_patterns || {};
    
    // 表引用正则
    renderPatternList('ref-patterns', regex.table_reference || []);
    
    // 表名清理模式
    renderPatternList('cleanup-patterns', regex.table_cleanup || []);
    
    // 文件扩展名
    document.getElementById('file_extension').value = regex.file_extension || '\\.(hql|sql)$';
}

// 渲染模式列表
function renderPatternList(containerId, patterns) {
    const container = document.getElementById(containerId);
    container.innerHTML = '';
    
    patterns.forEach((pattern, index) => {
        const input = document.createElement('input');
        input.type = 'text';
        input.className = 'w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-primary';
        input.value = pattern;
        input.dataset.index = index;
        container.appendChild(input);
    });
}

// 渲染过滤规则
function renderFilters() {
    const filters = config.filters || {};
    
    // 基础过滤选项
    document.getElementById('exclude_self_reference').checked = filters.exclude_self_reference || false;
    document.getElementById('exclude_same_layer').checked = filters.exclude_same_layer || false;
    
    // 排除模式
    renderPatternList('exclude-patterns', filters.exclude_patterns || []);
    
    // 包含模式
    renderPatternList('include-patterns', filters.include_patterns || []);
}

// 渲染输出配置
function renderOutput() {
    const output = config.output || {};
    
    // 基础列
    renderColumns('basic-columns-body', output.basic_columns || []);
    
    // 扩展列
    renderColumns('extended-columns-body', output.extended_columns || []);
}

// 渲染列配置
function renderColumns(tbodyId, columns) {
    const tbody = document.getElementById(tbodyId);
    tbody.innerHTML = '';
    
    columns.forEach(column => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td class="px-6 py-4 whitespace-nowrap">${column.name || ''}</td>
            <td class="px-6 py-4 whitespace-nowrap">${column.title || ''}</td>
            <td class="px-6 py-4 whitespace-nowrap">${column.width || ''}</td>
            <td class="px-6 py-4 whitespace-nowrap">${column.hidden ? '是' : '否'}</td>
        `;
        tbody.appendChild(row);
    });
}

// 渲染进度条配置
function renderProgress() {
    const progress = config.progress || {};
    document.getElementById('bar_length').value = progress.bar_length || 30;
    document.getElementById('show_percentage').checked = progress.show_percentage || true;
    document.getElementById('update_frequency').value = progress.update_frequency || 1;
}

// 保存配置
function saveConfig() {
    // 收集处理配置
    config.processing = {
        remove_suffix: document.getElementById('remove_suffix').value,
        suffix_identifier: document.getElementById('suffix_identifier').value,
        filter_schema: document.getElementById('filter_schema').value
    };
    
    // 收集正则表达式配置
    config.regex_patterns = {
        table_reference: collectPatterns('ref-patterns'),
        table_cleanup: collectPatterns('cleanup-patterns'),
        file_extension: document.getElementById('file_extension').value
    };
    
    // 收集过滤规则配置
    config.filters = {
        exclude_self_reference: document.getElementById('exclude_self_reference').checked,
        exclude_same_layer: document.getElementById('exclude_same_layer').checked,
        exclude_patterns: collectPatterns('exclude-patterns'),
        include_patterns: collectPatterns('include-patterns')
    };
    
    // 收集进度条配置
    config.progress = {
        bar_length: parseInt(document.getElementById('bar_length').value),
        show_percentage: document.getElementById('show_percentage').checked,
        update_frequency: parseInt(document.getElementById('update_frequency').value)
    };
    
    // 发送到服务器保存
    fetch('/api/config', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(config)
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            showMessage('配置已保存', 'success');
        } else {
            showMessage('保存失败: ' + data.message, 'error');
        }
    })
    .catch(error => {
        showMessage('保存失败: ' + error.message, 'error');
    });
}

// 收集模式列表
function collectPatterns(containerId) {
    const container = document.getElementById(containerId);
    const inputs = container.querySelectorAll('input');
    const patterns = [];
    
    inputs.forEach(input => {
        const value = input.value.trim();
        if (value) {
            patterns.push(value);
        }
    });
    
    return patterns;
}

// 上传配置
function uploadConfig(event) {
    const file = event.target.files[0];
    if (!file) return;
    
    const formData = new FormData();
    formData.append('file', file);
    
    fetch('/api/config/upload', {
        method: 'POST',
        body: formData
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            showMessage('配置文件已上传', 'success');
            loadConfig(); // 重新加载配置
        } else {
            showMessage('上传失败: ' + data.message, 'error');
        }
    })
    .catch(error => {
        showMessage('上传失败: ' + error.message, 'error');
    });
    
    // 重置文件输入
    event.target.value = '';
}

// 显示消息
function showMessage(message, type = 'success') {
    const messageDiv = document.getElementById('message');
    messageDiv.textContent = message;
    
    // 设置样式
    messageDiv.className = 'mb-6 p-4 rounded-md';
    if (type === 'success') {
        messageDiv.classList.add('bg-green-100', 'text-green-700');
    } else {
        messageDiv.classList.add('bg-red-100', 'text-red-700');
    }
    
    // 显示
    messageDiv.classList.remove('hidden');
    
    // 3秒后隐藏
    setTimeout(() => {
        messageDiv.classList.add('hidden');
    }, 3000);
}

// 项目管理相关函数
let currentEditingProject = null;

// 打开项目模态框
function openProjectModal() {
    currentEditingProject = null;
    document.getElementById('modal-title').textContent = '添加项目';
    document.getElementById('project-form').reset();
    document.getElementById('project-modal').classList.remove('hidden');
}

// 编辑项目
function editProject(projectName) {
    currentEditingProject = projectName;
    const project = config.projects[projectName];
    
    document.getElementById('modal-title').textContent = '编辑项目';
    document.getElementById('modal-name').value = projectName;
    document.getElementById('modal-prefix').value = project.prefix || '';
    document.getElementById('modal-theme').value = project.theme || '';
    document.getElementById('modal-description').value = project.description || '';
    
    document.getElementById('project-modal').classList.remove('hidden');
}

// 关闭项目模态框
function closeProjectModal() {
    document.getElementById('project-modal').classList.add('hidden');
    currentEditingProject = null;
}

// 保存项目
function saveProject(event) {
    event.preventDefault();
    
    const formData = new FormData(event.target);
    const project = {
        name: formData.get('name').trim(),
        prefix: formData.get('prefix').trim(),
        theme: formData.get('theme').trim(),
        description: formData.get('description').trim()
    };
    
    if (!project.name) {
        showMessage('项目名称不能为空', 'error');
        return;
    }
    
    // 初始化projects对象
    if (!config.projects) {
        config.projects = {};
    }
    
    // 如果是编辑现有项目且名称改变，需要删除旧项目
    if (currentEditingProject && currentEditingProject !== project.name) {
        delete config.projects[currentEditingProject];
    }
    
    // 保存项目
    config.projects[project.name] = {
        prefix: project.prefix,
        theme: project.theme,
        description: project.description
    };
    
    // 更新UI
    renderProjects();
    closeProjectModal();
    showMessage('项目已保存', 'success');
}

// 删除项目
function deleteProject(projectName) {
    if (confirm(`确定要删除项目 '${projectName}' 吗？`)) {
        delete config.projects[projectName];
        renderProjects();
        showMessage('项目已删除', 'success');
    }
}

// 正则表达式模式管理
function addRefPattern() {
    const container = document.getElementById('ref-patterns');
    const input = document.createElement('input');
    input.type = 'text';
    input.className = 'w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-primary';
    container.appendChild(input);
}

function removeRefPattern() {
    const container = document.getElementById('ref-patterns');
    const inputs = container.querySelectorAll('input');
    if (inputs.length > 0) {
        inputs[inputs.length - 1].remove();
    }
}

function addCleanupPattern() {
    const container = document.getElementById('cleanup-patterns');
    const input = document.createElement('input');
    input.type = 'text';
    input.className = 'w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-primary';
    container.appendChild(input);
}

function removeCleanupPattern() {
    const container = document.getElementById('cleanup-patterns');
    const inputs = container.querySelectorAll('input');
    if (inputs.length > 0) {
        inputs[inputs.length - 1].remove();
    }
}

// 过滤规则模式管理
function addExcludePattern() {
    const container = document.getElementById('exclude-patterns');
    const input = document.createElement('input');
    input.type = 'text';
    input.className = 'w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-primary';
    container.appendChild(input);
}

function removeExcludePattern() {
    const container = document.getElementById('exclude-patterns');
    const inputs = container.querySelectorAll('input');
    if (inputs.length > 0) {
        inputs[inputs.length - 1].remove();
    }
}

function addIncludePattern() {
    const container = document.getElementById('include-patterns');
    const input = document.createElement('input');
    input.type = 'text';
    input.className = 'w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-primary';
    container.appendChild(input);
}

function removeIncludePattern() {
    const container = document.getElementById('include-patterns');
    const inputs = container.querySelectorAll('input');
    if (inputs.length > 0) {
        inputs[inputs.length - 1].remove();
    }
}