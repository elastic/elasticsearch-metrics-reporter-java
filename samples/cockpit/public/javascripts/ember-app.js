// Ember App
App = Ember.Application.create();

// static metric type configuration
// first one is the default
App.metricTypes = {
   meter: [ 'm1_rate', 'count', 'mean_rate', 'm5_rate', 'm15_rate' ],
   timer: [ 'm1_rate', 'count', 'mean_rate', 'm5_rate', 'm15_rate', 'min', 'max', 'mean', 'p50', 'p75', 'p95', 'p98', 'p99', 'p999', 'stddev' ],
   counter: [ 'count' ],
   histogram: [ 'mean', 'count', 'min', 'max', 'p50', 'p75', 'p95', 'p98', 'p99', 'p999', 'stddev' ],
   gauge: [ 'value' ]
}

App.metricTimes = [ '1d', '1h', '12h', '1w', '1m' ]

App.percolatorOperators = [ '>', '>=', '<', '<=', '=',]

// models
App.Notification = Ember.Object.extend({metricName:null, percolatorId: null})
App.Percolation = Ember.Object.extend({ id: null, name: null, field: null, operator: null, value: null})
App.Graph = Ember.Object.extend({
    id: null,

    draw : function(metricName, metricValue, metricTime) {
        var anyParameterEmpty = _.some([metricName, metricValue, metricTime], _.isEmpty)
        if (anyParameterEmpty) return;

        var graph = _.find(App.metricNames, function(entry) { return entry.name === metricName })
        var id = this.get("id")
        
        var values = [ metricValue ]
        // Including min/max vastly destroys the graphs scale in my tests, so I left it out for now
        // if (_.contains(App.metricTypes[graph.type], 'max')) values.push('max') 
        // if (_.contains(App.metricTypes[graph.type], 'min')) values.push('min')

        $.getJSON('/graphData', {name: graph.name, type: graph.type, values: values, time: metricTime}, function(data) {

            nv.addGraph(function() {
                var chart = nv.models.lineWithFocusChart();
                chart.yAxis.tickFormat(d3.format(',.2f'));
                chart.y2Axis.tickFormat(d3.format(',.2f'));

                chart.xAxis.tickFormat(function (d) { return moment(d).format("HH:mm") });
                chart.x2Axis.tickFormat(function (d) { return moment(d).format("HH:mm") });

                d3.select("#" + id + ' svg')
                    .datum(data)
                    .transition().duration(500)
                    .call(chart);

                nv.utils.windowResize(chart.update);

                return chart;
            });
        });
    }
})

// list of all active notifications, graph init (would allow us to create arbitrary/many graphs!)
App.notifications = Ember.A();
App.graph = App.Graph.create({id: "chart"})

// start route
App.IndexRoute = Ember.Route.extend({
    model: function () {
        return App.notifications;
    }
});

App.NotificationController = Ember.ObjectController.extend({
    removeNotification: function () {
        var notification = this.get('model');
        App.notifications.removeObject(notification)
    },
    deletePercolation : function() {
        var notification = this.get('model');
        $.ajax({ url: '/percolator', data: { id: this.get('percolatorId') }, type: 'DELETE' })
        this.removeNotification()
    },
    drawMetric: function() {
        var notification = this.get('model');
        var metric = _.find(App.metricNames, function(entry) { return entry.name === notification.metricName })
        if (metric != undefined) {
            App.graph.draw(metric.name, App.metricTypes[metric.type][0])
        }
    }
});

App.IndexController = Ember.ArrayController.extend({
    drawGraph: function() {
        App.graph.draw(this.get('metricName'), this.get('metricValue'), this.get('metricTime'))
    },

    metricNameDidChange : Ember.observer(function() {
        this.setEmberSelectBox('metricName', 'metricValues', 'metricValue');
    },'metricName'),
    
    metricTimeDidChange : Ember.observer(function() {
        this.drawGraph();
    }, 'metricTime'),

    percolatorMetricNameDidChange : Ember.observer(function() {
        this.setEmberSelectBox('percolatorMetricName', 'percolatorFieldValues', 'percolatorFieldValue')
    },'percolatorMetricName'),

    setEmberSelectBox: function(inputFieldProperty, allowedValuesProperties, selectedAllowedValue) {
        var metricName = this.get(inputFieldProperty)
        if (metricName.length < 1) return;
        
        var metric = _.find(App.metricNames, function(entry) { return entry.name === metricName })
        if (metric !== undefined) {
            var allowedMetricValues = App.metricTypes[metric.type];
            this.set(allowedValuesProperties, allowedMetricValues)
            this.set(selectedAllowedValue, allowedMetricValues[0])
        }
    },

    savePercolation : function() {
        var that = this
        $.post('/percolator', { id: this.get('percolatorId'),
            name: this.get('percolatorMetricName'),
            field: this.get('percolatorFieldValue'),
            range: this.get('percolatorOperator'),
            rangeValue: this.get('percolatorValue')
        }).done(function() {
            $('#myModal').modal('hide')
            that.set('percolatorId', '')
            that.set('percolatorMetricName', '')
            that.set('percolatorFieldValue', '')
            that.set('percolatorOperator', '>')
            that.set('percolatorValue', '')
        })
    }
});

// On app load, load graph names
App.ready = function() {
    $.getJSON('/graphNames', function(data) {
        App.metricNames = data
        // not sure if this can be emberized.. works now
        $('#search').typeahead({source: _.map(data, function(val) { return val.name })})
        $('#metricName').typeahead({source: _.map(data, function(val) { return val.name })})
    })

    // SockJS init
    var sockjs = new SockJS('/ws');
    sockjs.onclose = function() { console.log("Web socket close, maybe reload?") }
    sockjs.onmessage = function(message) {
        var data = $.parseJSON(message.data)
        // add only if it is not yet in list
        var isNotificationAlreadyInList = App.notifications.some(function(item, index, enumerable) { return item.metricName == data.metricName })
        if (!isNotificationAlreadyInList) {
            App.notifications.pushObject(App.Notification.create({metricName: data.metricName, percolatorId: data.percolatorId }))
        }
    } 
}

