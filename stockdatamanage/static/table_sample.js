1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
18
19
20
21
22
23
24
25
26
27
28
29
30
31
32
33
34
35
36
37
38
39
40
41
42
43
44
45
46
47
48
49
50
51
52
53
54
55
56
57
58
59
60
61
62
63
64
65
66
67
68
69
70
71
72
73
74
75
76
77
78
79
80
81
82
83
84
85
86
87
88
89
90
91
92
93
94
95
96
97
98
99
100
101
102
103
104
105
106
107
108
109
110
111
112
113
114
115
116
117
118
119
120
121
122
123
124
125
126
127
128
129
130
131
132
133
134
135
136
137
138
139
140
141
142
143
144
145
146
147
148
149
150
151
152
153
154
155
156
157
var queryParams = function (params) {
    var param = {
        pageIndex: Math.ceil(params.offset / params.limit) + 1,
        pageSize: params.limit,
        order: params.order,
        ordername: params.sort,
        startDateTime: $('#dateSearch .startDate').val(),
        endDateTime: $('#dateSearch .endDate').val(),
        search: $('#dateSearch .imuserid').val(),
    }
    return param
}

var responseHandler = function (e) {
    console.log(e)
    if (e.data && e.data.length > 0) {
        return {'rows': e.data, 'total': e.count}
    } else {
        return {'rows': [], 'total': 0}
    }

}
var uidHandle = function (res) {
    var html = '<a href=\'#\'>' + res + '</a>'
    return html
}
var operateFormatter = function (value, row, index) {//赋予的参数
    return [
        '<button class="btn btn-info btn-sm rightSize detailBtn" type="button"><i class="fa fa-paste"></i> 详情</button>',
        '<button class="btn btn-danger btn-sm rightSize packageBtn" type="button"><i class="fa fa-envelope"></i> 通知</button>',
    ].join('')
}
self.dataInit = function (name) {
    var url, columns, tableName
    switch (name) {
        case 'loginLogTab':
            tableName = 'loginLog-table'
            columns = [
                {
                    checkbox: true,
                },
                {
                    field: 'uid',
                    title: '用户编号',
                    align: 'center',
                    formatter: uidHandle,//自定义方法设置uid跳转链接
                    width: 300,
                }, {
                    field: 'name',
                    title: '姓名',
                    align: 'center',
                    sortable: false,   //本列不可以排序
                }, {
                    field: 'sex',
                    title: '性别',
                    align: 'center',
                }, {
                    field: 'age',
                    title: '年龄',
                    align: 'center',
                    sortable: true,
                    clickToSelect: false,
                    sortName: 'age',
                    order: 'asc',
                }, {
                    field: 'area',
                    title: '户籍所在地',
                    align: 'left',
                    halign: 'center', //设置表头列居中对齐
                }, {
                    field: 'loginWay',
                    title: '登录方式',
                    align: 'center',
                }, {
                    field: 'status',
                    title: '状态',
                    align: 'center',
                }, {
                    field: 'createTime',
                    title: '登录时间',
                    align: 'center',
                    width: 90,
                }, {
                    field: 'orderService',
                    title: '购买服务',
                    align: 'center',
                }, {
                    field: 'connectorIP',
                    title: '连接器IP',
                    align: 'center',
                }, {
                    field: 'connectorPort',
                    title: '连接器端口',
                    align: 'center',
                }, {
                    field: 'operate',
                    title: '操作',
                    align: 'center',
                    valign: 'middle',
                    formatter: operateFormatter, //自定义方法，添加操作按钮
                },
            ]
            break
        case 'receiveLogTab':
            //省略
            break
        case 'socketInputTab':
            //省略
            break
        case 'socketOutputTab':
            //省略
            break
    }
    $('#' + tableName).empty()
    $('#' + tableName).bootstrapTable('destroy').bootstrapTable({
        url: '../data/login_info2.json',   //url一般是请求后台的url地址,调用ajax获取数据。此处我用本地的json数据来填充表格。
        method: 'get',                     //使用get请求到服务器获取数据
        dataType: 'json',
        contentType: 'application/json,charset=utf-8',
        toolbar: '#toolbar',                //一个jQuery 选择器，指明自定义的toolbar 例如:#toolbar, .toolbar.
        uniqueId: 'id',                    //每一行的唯一标识，一般为主键列
        height: document.body.clientHeight - 165,   //动态获取高度值，可以使表格自适应页面
        cache: false,                       // 不缓存
        striped: true,                      // 隔行加亮
        queryParamsType: 'limit',           //设置为"undefined",可以获取pageNumber，pageSize，searchText，sortName，sortOrder
                                            //设置为"limit",符合 RESTFul 格式的参数,可以获取limit, offset, search, sort, order
        queryParams: queryParams,
        sidePagination: 'server',           //分页方式：client客户端分页，server服务端分页（*）
        // sortable: true,                     //是否启用排序;意味着整个表格都会排序
        sortName: 'uid',                    // 设置默认排序为 name
        sortOrder: 'asc',                   //排序方式
        pagination: true,                   //是否显示分页（*）
        search: true,                       //是否显示表格搜索，此搜索是客户端搜索，不会进服务端，所以，个人感觉意义不大
        strictSearch: true,
        showColumns: true,                  //是否显示所有的列
        showRefresh: true,                  //是否显示刷新按钮
        showToggle: true,                    //是否显示详细视图和列表视图
        clickToSelect: true,                //是否启用点击选中行
        minimumCountColumns: 2,          //最少允许的列数 clickToSelect: true, //是否启用点击选中行
        pageNumber: 1,                   //初始化加载第一页，默认第一页
        pageSize: 10,                    //每页的记录行数（*）
        pageList: [10, 25, 50, 100],     //可供选择的每页的行数（*）
        paginationPreText: 'Previous',
        paginationNextText: 'Next',
        paginationFirstText: 'First',
        paginationLastText: 'Last',
        responseHandler: responseHandler,
        columns: columns,
        onLoadSuccess: function (data) { //加载成功时执行
            console.log(data)
        },
        onLoadError: function (res) { //加载失败时执行
            console.log(res)
        },
    })
}