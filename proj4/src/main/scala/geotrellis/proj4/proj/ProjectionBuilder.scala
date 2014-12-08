package geotrellis.proj4.proj

trait ProjectionBuilder {
  def apply(params: ProjectionParams): Projection
}

object ProjectionBuilder {
  def apply(params: ProjectionParams): Projection = {
    val builder = registry.getProjectionbuilder(name)

        var projection: org.osgeo.proj4j.proj.Projection = null;

        String s;
        s = (String)params.get( Proj4Keyword.proj );
        if ( s != null ) {
          projection = registry.getProjection( s );
          if ( projection == null )
            throw new InvalidValueException( "Unknown projection: "+s );
        }

        projection.setEllipsoid(ellipsoid);
    
        //TODO: better error handling for things like bad number syntax.
        // Should be able to report the original param string in the error message
        // Should the exception be lib-specific?  (e.g. ParseException)
    
        s = (String)params.get( Proj4Keyword.alpha );
        if ( s != null )
          projection.setAlphaDegrees( Double.parseDouble( s ) );
    
        s = (String)params.get( Proj4Keyword.lonc );
        if ( s != null )
          projection.setLonCDegrees( Double.parseDouble( s ) );
    
        s = (String)params.get( Proj4Keyword.lat_0 );
        if ( s != null )
          projection.setProjectionLatitudeDegrees( parseAngle( s ) );
    
        s = (String)params.get( Proj4Keyword.lon_0 );
        if ( s != null )
          projection.setProjectionLongitudeDegrees( parseAngle( s ) );
    
        s = (String)params.get( Proj4Keyword.lat_1 );
        if ( s != null )
          projection.setProjectionLatitude1Degrees( parseAngle( s ) );
    
        s = (String)params.get( Proj4Keyword.lat_2 );
        if ( s != null )
          projection.setProjectionLatitude2Degrees( parseAngle( s ) );
    
        s = (String)params.get( Proj4Keyword.lat_ts );
        if ( s != null )
          projection.setTrueScaleLatitudeDegrees( parseAngle( s ) );
    
        s = (String)params.get( Proj4Keyword.x_0 );
        if ( s != null )
          projection.setFalseEasting( Double.parseDouble( s ) );
    
        s = (String)params.get( Proj4Keyword.y_0 );
        if ( s != null )
          projection.setFalseNorthing( Double.parseDouble( s ) );

        s = (String)params.get( Proj4Keyword.k_0 );
        if ( s == null )
          s = (String)params.get( Proj4Keyword.k );
        if ( s != null )
          projection.setScaleFactor( Double.parseDouble( s ) );

        s = (String)params.get( Proj4Keyword.units );
        if ( s != null ) {
          Unit unit = Units.findUnits( s );
          // TODO: report unknown units name as error
          if ( unit != null ) {
            projection.setFromMetres( 1.0 / unit.value );
            projection.setUnits( unit );
          }
        }
    
        s = (String)params.get( Proj4Keyword.to_meter );
        if ( s != null )
          projection.setFromMetres( 1.0/Double.parseDouble( s ) );

        if ( params.containsKey( Proj4Keyword.south ) )
          projection.setSouthernHemisphere(true);

        //TODO: implement some of these parameters ?
    
        // this must be done last, since behaviour depends on other params being set (eg +south)
        if (projection instanceof TransverseMercatorProjection) {
          s = (String) params.get(Proj4Keyword.zone);
          if (s != null)
            ((TransverseMercatorProjection) projection).setUTMZone(Integer
              .parseInt(s));
        }

        projection.initialize();

        return projection;

  }
}
