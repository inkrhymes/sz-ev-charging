# 前端改版规范：深圳市电动汽车充电站运行管理可视化系统

适用文件：`web/index.html`（纯 HTML/CSS + ECharts 5，不引入 React/Vue）
执行人：小黄（唯一改动人）；其他人只提意见。

## 1. 现状问题（为什么看着"AI味"）
基于 main@8062ec7 的 `web/index.html` 逐条列出：
1. **深蓝黑底 + 亮色字**（`--bg:#0b1220; --card:#121a2b`）：这是 AI 生成大屏的默认配色。真实运营后台以浅色为主，深色大屏只用在投屏场景。
2. **没有信息架构**：一页平铺 6 个卡片，没有侧边导航、面包屑和页面标题区，不像"系统"，更像一张"海报"。
3. **把开发术语直接露给用户**：标题区写着 `overview / zone / quality / baseline / tickets`、"工单 stub"、"只读登录 stub"、"naive 基线"、`readonly/admin`、`row_missing_rate` 原始字段名，还有 MOCK/LIVE 胶囊徽章。
4. **卡片样式千篇一律**：10px 圆角、1px 描边，KPI 只有"标签+大数字"，没有单位、对比和口径说明。
5. **质检用柱状图展示 4 个量纲不同的数字**（比率和个数混在一张图里），可读性差，属于"为了有图而画图"。
6. **工单表的按钮是亮蓝实心小按钮**，状态直接显示英文（open/processing/done），没有状态标签色。
7. **登录被做成页面中间的一个卡片**，输出是 `<pre>` JSON。真实系统的登录要么是独立页，要么放在右上角用户菜单里。
8. 字体只写了 `"Segoe UI"`，中文字体会回退得不一致；字号层级只有 18/14/12/11，层级太扁。

## 2. 改版方向：类 Arco Design Pro 的浅色企业控制台
选这个方向的理由：Arco 是字节跳动的设计系统，面试官一眼就认得出"规范的中后台"；另外它只需要用 CSS 变量就能复刻，不依赖组件库。

### 2.1 颜色 Token（取自 Arco 默认主题）
| Token | Hex | 用途 |
|---|---|---|
| --primary-6 | #165DFF | 主色：主按钮、选中菜单、链接、主折线 |
| --primary-1 | #E8F3FF | 选中菜单底色、浅标签底 |
| --primary-5 | #4080FF | hover |
| --primary-7 | #0E42D2 | active |
| --success-6 | #00B42A | 已完成、正常 |
| --success-1 | #E8FFEA | |
| --warning-6 | #FF7D00 | 高缺失、处理中 |
| --warning-1 | #FFF7E8 | |
| --danger-6 | #F53F3F | 卡死、待处理维修 |
| --danger-1 | #FFECE8 | |
| --text-1 | #1D2129 | 标题、正文主色 |
| --text-2 | #4E5969 | 次要正文、表格内容 |
| --text-3 | #86909C | 说明、坐标轴标签 |
| --text-4 | #C9CDD4 | 禁用 |
| --border | #E5E6EB | 卡片和表格分割线 |
| --fill-1 | #F7F8FA | 表头、悬停行 |
| --fill-2 | #F2F3F5 | 页面背景 |
| --bg-white | #FFFFFF | 卡片、顶栏、侧栏 |

全站只用这一套颜色。**禁止渐变**和半透明发光色。

### 2.2 字体与字号
`font-family: -apple-system, BlinkMacSystemFont, "PingFang SC", "Microsoft YaHei", "Helvetica Neue", Arial, sans-serif;`
数字列和 KPI 数值加 `font-variant-numeric: tabular-nums;`

| 层级 | 字号/行高 | 字重 |
|---|---|---|
| 页面标题 | 20px / 28px | 500 |
| 卡片标题 | 16px / 24px | 500 |
| 正文/表格 | 14px / 22px | 400 |
| 辅助说明 | 12px / 20px | 400 |
| KPI 数值 | 28px / 36px | 600 |

### 2.3 间距、圆角、阴影
- 栅格基准 4px，常用间距为 4 / 8 / 12 / 16 / 20 / 24。
- 内容区内边距 20px；卡片之间的间距 16px；卡片内边距 20px（头部 16px 20px）。
- 圆角：卡片 4px，按钮/输入框 2px，标签 2px。不要再用 10px 或 999px。
- 阴影：卡片默认**没有阴影**，只靠 `1px solid #E5E6EB` 或白底和灰底的对比区分层次；下拉层和弹窗用 `0 4px 10px rgba(0,0,0,.1)`。

### 2.4 布局骨架
```
┌──────────── 顶栏 60px（白底，底边 1px 描边）──────────────┐
│ Logo+系统名 │                    数据日期 · 刷新 · 用户头像▼ │
├────────┬──────────────────────────────────────────────────┤
│ 侧栏    │ 面包屑：运行监控 / 运行总览                          │
│ 220px  │ 页面标题 20px + 一行说明                            │
│ 可折叠  │ ┌─卡片─┐ ┌─卡片─┐ …                                 │
│ 为 48px │                                                  │
└────────┴──────────────────────────────────────────────────┘
```
- 侧栏：白底，菜单项高 40px，选中态为 `--primary-1` 底、`--primary-6` 字，左侧加一条 3px 主色竖条；图标统一用 16px 线性 SVG（可从 Arco icon 或 Tabler 内联），**禁止 emoji**。
- 1280 宽：侧栏 220 + 内容 1060（减去 20×2 内边距后约 1020px）。KPI 一行 4 张（2 行），图表区为 2 列（12+12 栅格）。
- 1920 宽：内容区 `max-width:1600px`，KPI 一行 6 张，图表区为 16+8 两列（主图+侧栏卡片）。
- 小于 1100px 时侧栏自动折叠成 48px，只显示图标。
- 用 CSS Grid 24 栅格：`.grid{display:grid;grid-template-columns:repeat(24,minmax(0,1fr));gap:16px}`，再用 `.col-12`、`.col-8` 之类的类控制宽度。

### 2.5 KPI 卡规范
```
┌────────────────────────────┐
│ 平均占用率          ⓘ        │ 12px text-3；ⓘ 悬停显示口径
│ 42.0 %                      │ 28px 600 text-1，单位 14px text-2
│ 较前一日 ↑ 3.2%  │ 峰值时段 19:00 │ 12px，涨跌用 success/danger 色
└────────────────────────────┘
```
- 白底，`1px #E5E6EB` 描边，4px 圆角，内边距 20px，高度 112px。
- 异常类 KPI（卡死站、高缺失站）的数值用 danger/warning 色，**不要给整张卡片加底色**。
- 可选：右侧放 64×32 的迷你折线（ECharts，不显示坐标轴）。

### 2.6 表格与标签
- 表头底色 `#F7F8FA`、字色 text-2、字重 500、行高 40px；单元格行高 48px，只保留横向分割线 `#E5E6EB`，悬停行 `#F7F8FA`。
- 状态用 Tag 组件：高 24px、内边距 0 8px、2px 圆角、浅底深字。
  - 待处理：`danger-1` 底 / `danger-6` 字
  - 处理中：`warning-1` / `warning-6`
  - 已完成：`success-1` / `success-6`
  - 巡检/维修类型：灰底 `#F2F3F5` / text-2
- 操作列用文字按钮（主色字、无底色），如「流转到下一状态」；不用实心小蓝按钮。
- 按钮：主按钮 32px 高、`#165DFF` 底；次按钮白底加 `#E5E6EB` 描边。

### 2.7 ECharts 主题（用 `echarts.registerTheme('arcoLight', …)` 一次注册）
```js
{
  color: ['#165DFF','#14C9C9','#F7BA1E','#722ED1','#F53F3F','#00B42A'],
  backgroundColor: 'transparent',
  textStyle: { fontFamily: 'inherit', color: '#4E5969', fontSize: 12 },
  title: { show: false },              // 标题统一写在卡片头，不放在图里
  grid: { left: 48, right: 16, top: 40, bottom: 32, containLabel: false },
  legend: { top: 0, right: 0, icon: 'roundRect', itemWidth: 12, itemHeight: 4,
            textStyle: { color: '#4E5969' } },
  categoryAxis: { axisLine:{lineStyle:{color:'#E5E6EB'}}, axisTick:{show:false},
                  axisLabel:{color:'#86909C'}, splitLine:{show:false} },
  valueAxis:    { axisLine:{show:false}, axisTick:{show:false},
                  axisLabel:{color:'#86909C'},
                  splitLine:{lineStyle:{color:'#E5E6EB', type:'dashed'}} },
  line: { symbol:'none', smooth:false, lineStyle:{width:2} },
  bar:  { barMaxWidth: 24, itemStyle:{ borderRadius:[2,2,0,0] } },
  tooltip: { trigger:'axis', backgroundColor:'#fff', borderColor:'#E5E6EB',
             borderWidth:1, padding:[8,12], textStyle:{color:'#1D2129',fontSize:12},
             extraCssText:'box-shadow:0 4px 10px rgba(0,0,0,.1);border-radius:4px;',
             axisPointer:{ type:'line', lineStyle:{color:'#C9CDD4', type:'dashed'} } }
}
```
- 面积图只允许用主色 8% 透明度的**纯色**填充，不用渐变。
- 占用率坐标轴显示百分比（`formatter: v => (v*100).toFixed(0)+'%'`），时间轴写 `MM-DD HH:mm`。
- 图表高度：主图 320px，次图 260px。要监听窗口 resize，侧栏折叠时也要触发 `chart.resize()`。

## 3. 页面结构与导航（对应现有功能）
单页内用 hash 路由（`#/overview` 等），各视图用 `display` 切换，不引入框架。

| 侧栏菜单 | 面包屑 | 内容 | 用到的现有接口 |
|---|---|---|---|
| 运行监控 › 运行总览 | 运行监控 / 运行总览 | KPI 卡 6 张（站点数、片区数「23（18 个有效）」、平均占用率、峰值时段、卡死站、高缺失站）+ 全市占用率趋势 + 工单概况 | /api/overview, /api/zones |
| 运行监控 › 片区分析 | 运行监控 / 片区分析 | 筛选栏（片区下拉、日期范围、查询/重置按钮）+ 占用率与估算负荷双轴图 + 明细表 | /api/zone/series (from/to) |
| 运行监控 › 基线对比 | 运行监控 / 基线对比 | 实际值与「上一时刻基线」预测折线 + 绝对误差柱图 + MAE 指标卡 | /api/baseline |
| 数据治理 › 数据质量 | 数据治理 / 数据质量 | 4 张质检指标卡（缺失率 26.42%、卡死站 4、高缺失站 6、样本站 25）+ 异常站点表（站点、片区、问题类型 Tag、缺失率） | /api/quality |
| 运维管理 › 运维工单 | 运维管理 / 运维工单 | 状态统计 Tab（全部/待处理/处理中/已完成）+ 工单表 + 文字按钮流转 | /api/tickets |
| 右上角用户菜单 | — | 头像下拉里显示当前用户和角色「只读访客」；登录改成独立的简洁登录页或弹窗（账号、角色、登录按钮），只读角色下隐藏流转按钮 | /api/login |

- 数据来源状态（原 MOCK/LIVE）放到顶栏右侧，写成「数据源：MySQL 实时 / 离线样例」的小号灰字加 6px 状态圆点，不要用彩色胶囊。
- 顶栏显示「数据窗口：2022-09-01 至 2022-09-14」。
- 每张卡片头右侧放 ⓘ 口径说明（悬停 tooltip），这是面试时最好讲的细节。

## 4. 必须删掉或替换的内容
- 深色背景 `#0b1220/#121a2b` 和所有深色变量（如果想保留投屏用的深色版，留到二期，本次不做）。
- 所有渐变、`box-shadow` 发光、`backdrop-filter`、霓虹色描边、999px 胶囊徽章。
- emoji 和装饰性符号（⚡🔌📊 等），包括按钮和标题里的。
- 面向开发者的文案：stub、naive、mock、`overview / zone / …` 副标题、英文字段名（`row_missing_rate` 改成「数据缺失率」）、英文状态（改成中文 Tag）、`<pre>` 里的原始 JSON 输出。
- 质检柱状图：改成指标卡加异常站点表。
- 标题里的「（演示）」：`<title>` 改成「深圳市电动汽车充电站运行管理可视化系统」。
- 不需要的动效：只保留 150ms 的 hover 过渡，去掉数字滚动和脉冲动画。

## 5. 参考来源与借鉴点
| 参考 | 链接 | 借鉴什么 |
|---|---|---|
| Arco Design Pro（字节） | https://arco.design/pro 、https://github.com/arco-design/arco-design-pro | 整体骨架（60px 顶栏、220px 侧栏、面包屑）、色板、工作台页的指标卡和图表卡样式 |
| Arco 设计令牌 | https://arco.design/react/docs/token 、https://arco.design/palette/list | 2.1 节色值、字号、圆角 |
| Arco 数据可视化规范 | https://arco.design/docs/spec/data-visualization | 图表配色顺序、坐标轴和 tooltip 的克制风格 |
| Ant Design Pro | https://preview.pro.ant.design/dashboard/analysis | 分析页 KPI 卡的「标题+数值+环比+底部说明」结构，以及筛选栏和表格页 |
| TDesign Starter（腾讯） | https://tdesign.tencent.com/starter/vue-next/ | 统计卡加迷你趋势图、侧栏折叠交互、列表页 Tab 统计 |
| Element Plus 后台（vue-element-admin / vue-pure-admin） | https://github.com/PanJiaChen/vue-element-admin 、https://pure-admin.github.io/vue-pure-admin | 国内常见后台的信息密度、标签页和表格操作列写法 |
| Grafana | https://play.grafana.org | 时序图的克制做法：细线、虚线网格、tooltip 可同时对比多条序列；面板头部的 ⓘ 说明 |
| 阿里云 DataV | https://datav.aliyun.com | 只作为反例和二期参考：投屏大屏才用深色，日常运营后台不用 |
| 特来电 / 星星充电 / e充电 | https://www.teld.cn 、https://www.starcharge.com 、https://www.echargenet.com | 业务术语（场站、终端、充电量、利用率、运维工单），菜单可按「场站运营 / 运维 / 数据」分组；它们的运营平台需要登录，公开页面只能借鉴术语 |

## 本地参考截图
- `refs/antd_pro_preview.png`：Ant Design Pro 官方 README 的预览图（分析页）
- `refs/tdesign_starter_preview.png`：TDesign Vue Next Starter 官方 README 的预览图

## 验收标准（小黄改完后自查）
1. 1280×800 和 1920×1080 下都没有横向滚动条，KPI 不会被裁掉。
2. 全文件 `grep -E "gradient|#0b1220|stub|naive|emoji"` 结果为空。
3. 所有颜色都来自 2.1 节的 Token。
4. 只读角色看不到工单流转按钮。
5. 侧栏 5 个菜单都能切换，刷新页面后能停留在当前 hash。
