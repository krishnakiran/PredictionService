<%@page import="java.util.ArrayList"%>
<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
<head>
<title>Taxi Cab Fraud Analysis</title>
<meta charset="utf-8">
<link rel="stylesheet" href="bootstrap/css/bootstrap.css" type="text/css" />
<link rel="stylesheet" href="http://code.jquery.com/ui/1.10.4/themes/smoothness/jquery-ui.css">
<link rel="stylesheet" href="charts/nv.d3.css" type="text/css" />
<script src="http://code.jquery.com/jquery-1.10.2.js"></script>
<script src="http://code.jquery.com/ui/1.10.4/jquery-ui.js"></script>
<script src="bootstrap/js/bootstrap.js"></script>
<script src="charts/d3.min.js"></script>
<script src="charts/nv.d3.min.js""></script>
<script>
$(function() {
$( "#accordion" ).accordion();
});
</script>
</head>

<body>
	<div class="container">
		<br />
		<h1>
			<img src="bootstrap/img/bludemo.jpg"><a href="index.html">Taxi Cab Fraud Analysis</a>
			<a href="index.html" class="btn btn-info"><i class="icon-white icon-home center"></i>Home</a>
		</h1>
			<div id="accordion" class="container">
				<h3>Top-5 cost outlier Report</h2>
					<div class="container">
						<table class="table" id="results" cellspacing='15'>
							<thead>
								<tr style="font-size: 18px;"/>
							</thead>
							<tbody>
								<%
								ArrayList<double[]> outputList = (ArrayList<double[]>) session
											.getAttribute("outputList");
								for (int i = 0; i < outputList.size(); i++) 
								{
								%>
								<tr>
									<%-- <td><%=i + 1%></td> --%>
										<% 
											double[] entry=outputList.get(i);
											for(int j=0;j<entry.length-1;j++)
											{
										%>
												<td><%=entry[j]%></td>
												
									   <%
											}
										%>
								</tr>
								<%
								}
								%>
							</tbody>
						</table>
 				 </div>
				 <h3>Clustering Report</h2>
				 <div class="container">
				</div>
			</div>
			<div class="span9">
					<div id='chart'>
 							<svg style='height:500px'> </svg>
					</div>
					
					<script>
					nv.addGraph(function() {
						  var chart = nv.models.scatterChart()
						                .showDistX(true)    //showDist, when true, will display those little distribution lines on the axis.
						                .showDistY(true)
						                .transitionDuration(350)
						                .color(d3.scale.category10().range());
						  //Configure how the tooltip looks.
						  chart.tooltipContent(function(key) {
						      return '<h3>' + key + '</h3>';
						  });

						  //Axis settings
						  chart.xAxis.tickFormat(d3.format('.02f'));
						  chart.yAxis.tickFormat(d3.format('.02f'));

						  //We want to show shapes other than circles.
						  chart.scatter.onlyCircles(false);
						  
						  d3.json("http://localhost:8080/MLDemo/json", function(error, data) {
							  //myData=data;
							  d3.select('#chart svg')
						      .datum(data)
						      .call(chart);
						      nv.utils.windowResize(chart.update);
						  });
						  //var myData = randomData(4,40);
						  return chart;
						});

					</script>
			</div>
	</div>
</body>
</html>
