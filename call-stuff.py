from subprocess import call

calls = [

"""./sbt "project spark" "run --sparkMaster spark://ip-172-31-13-123.ec2.internal:7077 --sparkOpts spark.executor.memory=2g --zookeeper ip-172-31-13-123.ec2.internal --instance gis --user root --password secret --crs EPSG:3857 --pyramid true --clobber true --input s3n://AKIAJG2PMVWCTRRE4BCQ:5wqgZOYyY3F3FDG1QWND85EpKHKiskAeWPn1927y@ipcc5-models/monthly/pr/giss-e2-r/rcp45/BCSD_0.5deg_pr_Amon_giss-e2-r_rcp45_r1i1p1_200601-210012.nc --layerName giss-e2-r_rcp45 --table climate_precip" """,

"""./sbt "project spark" "run --sparkMaster spark://ip-172-31-13-123.ec2.internal:7077 --sparkOpts spark.executor.memory=2g --zookeeper ip-172-31-13-123.ec2.internal --instance gis --user root --password secret --crs EPSG:3857 --pyramid true --clobber true --input s3n://AKIAJG2PMVWCTRRE4BCQ:5wqgZOYyY3F3FDG1QWND85EpKHKiskAeWPn1927y@ipcc5-models/monthly/pr/giss-e2-r/rcp85/BCSD_0.5deg_pr_Amon_giss-e2-r_rcp85_r1i1p1_200601-210012.nc --layerName giss-e2-r_rcp85 --table climate_precip" """,
]

"""./sbt "project spark" "run --sparkMaster spark://ip-172-31-13-123.ec2.internal:7077 --sparkOpts spark.executor.memory=4g --zookeeper ip-172-31-13-123.ec2.internal --instance gis --user root --password secret --crs EPSG:3857 --pyramid true --clobber true --input s3n://AKIAJG2PMVWCTRRE4BCQ:5wqgZOYyY3F3FDG1QWND85EpKHKiskAeWPn1927y@ipcc5-models/monthly/pr/miroc5/rcp26/BCSD_0.5deg_pr_Amon_miroc5_rcp26_r1i1p1_200601-210012.nc --layerName miroc5_rcp26 --table climate_precip" """,

"""./sbt "project spark" "run --sparkMaster spark://ip-172-31-13-123.ec2.internal:7077 --sparkOpts spark.executor.memory=4g --zookeeper ip-172-31-13-123.ec2.internal --instance gis --user root --password secret --crs EPSG:3857 --pyramid true --clobber true --input s3n://AKIAJG2PMVWCTRRE4BCQ:5wqgZOYyY3F3FDG1QWND85EpKHKiskAeWPn1927y@ipcc5-models/monthly/pr/miroc5/rcp45/BCSD_0.5deg_pr_Amon_miroc5_rcp45_r1i1p1_200601-210012.nc --layerName miroc5_rcp45 --table climate_precip" """,

"""./sbt "project spark" "run --sparkMaster spark://ip-172-31-13-123.ec2.internal:7077 --sparkOpts spark.executor.memory=4g --zookeeper ip-172-31-13-123.ec2.internal --instance gis --user root --password secret --crs EPSG:3857 --pyramid true --clobber true --input s3n://AKIAJG2PMVWCTRRE4BCQ:5wqgZOYyY3F3FDG1QWND85EpKHKiskAeWPn1927y@ipcc5-models/monthly/pr/miroc5/rcp60/BCSD_0.5deg_pr_Amon_miroc5_rcp60_r1i1p1_200601-210012.nc --layerName miroc5_rcp60 --table climate_precip" """,

"""./sbt "project spark" "run --sparkMaster spark://ip-172-31-13-123.ec2.internal:7077 --sparkOpts spark.executor.memory=4g --zookeeper ip-172-31-13-123.ec2.internal --instance gis --user root --password secret --crs EPSG:3857 --pyramid true --clobber true --input s3n://AKIAJG2PMVWCTRRE4BCQ:5wqgZOYyY3F3FDG1QWND85EpKHKiskAeWPn1927y@ipcc5-models/monthly/pr/miroc5/rcp85/BCSD_0.5deg_pr_Amon_miroc5_rcp85_r1i1p1_200601-210012.nc --layerName miroc5_rcp85 --table climate_precip" """,


"""./sbt "project spark" "run --sparkMaster spark://ip-172-31-13-123.ec2.internal:7077 --sparkOpts spark.executor.memory=4g --zookeeper ip-172-31-13-123.ec2.internal --instance gis --user root --password secret --crs EPSG:3857 --pyramid true --clobber true --input s3n://AKIAJG2PMVWCTRRE4BCQ:5wqgZOYyY3F3FDG1QWND85EpKHKiskAeWPn1927y@ipcc5-models/monthly/pr/giss-e2-r/rcp26/BCSD_0.5deg_pr_Amon_giss-e2-r_rcp26_r1i1p1_200601-210012.nc --layerName giss-e2-r_rcp26 --table climate_precip" """,

"""./sbt "project spark" "run --sparkMaster spark://ip-172-31-13-123.ec2.internal:7077 --sparkOpts spark.executor.memory=4g --zookeeper ip-172-31-13-123.ec2.internal --instance gis --user root --password secret --crs EPSG:3857 --pyramid true --clobber true --input s3n://AKIAJG2PMVWCTRRE4BCQ:5wqgZOYyY3F3FDG1QWND85EpKHKiskAeWPn1927y@ipcc5-models/monthly/pr/giss-e2-r/rcp45/BCSD_0.5deg_pr_Amon_giss-e2-r_rcp45_r1i1p1_200601-210012.nc --layerName giss-e2-r_rcp45 --table climate_precip" """,

"""./sbt "project spark" "run --sparkMaster spark://ip-172-31-13-123.ec2.internal:7077 --sparkOpts spark.executor.memory=4g --zookeeper ip-172-31-13-123.ec2.internal --instance gis --user root --password secret --crs EPSG:3857 --pyramid true --clobber true --input s3n://AKIAJG2PMVWCTRRE4BCQ:5wqgZOYyY3F3FDG1QWND85EpKHKiskAeWPn1927y@ipcc5-models/monthly/pr/giss-e2-r/rcp60/BCSD_0.5deg_pr_Amon_giss-e2-r_rcp60_r1i1p1_200601-210012.nc --layerName giss-e2-r_rcp60 --table climate_precip" """,

"""./sbt "project spark" "run --sparkMaster spark://ip-172-31-13-123.ec2.internal:7077 --sparkOpts spark.executor.memory=4g --zookeeper ip-172-31-13-123.ec2.internal --instance gis --user root --password secret --crs EPSG:3857 --pyramid true --clobber true --input s3n://AKIAJG2PMVWCTRRE4BCQ:5wqgZOYyY3F3FDG1QWND85EpKHKiskAeWPn1927y@ipcc5-models/monthly/pr/giss-e2-r/rcp85/BCSD_0.5deg_pr_Amon_giss-e2-r_rcp85_r1i1p1_200601-210012.nc --layerName giss-e2-r_rcp85 --table climate_precip" """,


"""./sbt "project spark" "run --sparkMaster spark://ip-172-31-13-123.ec2.internal:7077 --sparkOpts spark.executor.memory=4g --zookeeper ip-172-31-13-123.ec2.internal --instance gis --user root --password secret --crs EPSG:3857 --pyramid true --clobber true --input s3n://AKIAJG2PMVWCTRRE4BCQ:5wqgZOYyY3F3FDG1QWND85EpKHKiskAeWPn1927y@ipcc5-models/monthly/pr/giss-e2-r/rcp26/BCSD_0.5deg_pr_Amon_giss-e2-r_rcp26_r1i1p1_200601-210012.nc --layerName giss-e2-r_rcp26 --table climate_precip" """,

"""./sbt "project spark" "run --sparkMaster spark://ip-172-31-13-123.ec2.internal:7077 --sparkOpts spark.executor.memory=4g --zookeeper ip-172-31-13-123.ec2.internal --instance gis --user root --password secret --crs EPSG:3857 --pyramid true --clobber true --input s3n://AKIAJG2PMVWCTRRE4BCQ:5wqgZOYyY3F3FDG1QWND85EpKHKiskAeWPn1927y@ipcc5-models/monthly/pr/giss-e2-r/rcp45/BCSD_0.5deg_pr_Amon_giss-e2-r_rcp45_r1i1p1_200601-210012.nc --layerName giss-e2-r_rcp45 --table climate_precip" """,

"""./sbt "project spark" "run --sparkMaster spark://ip-172-31-13-123.ec2.internal:7077 --sparkOpts spark.executor.memory=4g --zookeeper ip-172-31-13-123.ec2.internal --instance gis --user root --password secret --crs EPSG:3857 --pyramid true --clobber true --input s3n://AKIAJG2PMVWCTRRE4BCQ:5wqgZOYyY3F3FDG1QWND85EpKHKiskAeWPn1927y@ipcc5-models/monthly/pr/giss-e2-r/rcp60/BCSD_0.5deg_pr_Amon_giss-e2-r_rcp60_r1i1p1_200601-210012.nc --layerName giss-e2-r_rcp60 --table climate_precip" """,

"""./sbt "project spark" "run --sparkMaster spark://ip-172-31-13-123.ec2.internal:7077 --sparkOpts spark.executor.memory=4g --zookeeper ip-172-31-13-123.ec2.internal --instance gis --user root --password secret --crs EPSG:3857 --pyramid true --clobber true --input s3n://AKIAJG2PMVWCTRRE4BCQ:5wqgZOYyY3F3FDG1QWND85EpKHKiskAeWPn1927y@ipcc5-models/monthly/pr/giss-e2-r/rcp85/BCSD_0.5deg_pr_Amon_giss-e2-r_rcp85_r1i1p1_200601-210012.nc --layerName giss-e2-r_rcp85 --table climate_precip" """
]

for c in calls:
  call(c, shell=True)
