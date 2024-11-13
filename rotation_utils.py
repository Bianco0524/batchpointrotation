
def get_reconstruction_model_dict(MODEL_NAME):

    if MODEL_NAME == 'SETON2012':
        model_dict = {'RotationFile': ['Seton_etal_ESR2012_2012.1.rot'],
                      'Coastlines': 'coastlines_low_res/Seton_etal_ESR2012_Coastlines_2012.shp',
                      'StaticPolygons': 'Seton_etal_ESR2012_StaticPolygons_2012.1.gpmlz',
                      'PlatePolygons': ['Seton_etal_ESR2012_PP_2012.1.gpmlz'],
                      'ValidTimeRange': [200., 0.]}

    elif MODEL_NAME == 'MULLER2016':
        model_dict = {'RotationFile': ['Global_EarthByte_230-0Ma_GK07_AREPS.rot'],
                      'Coastlines': 'Global_EarthByte_230-0Ma_GK07_AREPS_Coastlines.gpmlz',
                      'StaticPolygons': 'Global_EarthByte_GPlates_PresentDay_StaticPlatePolygons_2015_v1.gpmlz',
                      'PlatePolygons': ['Global_EarthByte_230-0Ma_GK07_AREPS_PlateBoundaries.gpmlz',
                                        'Global_EarthByte_230-0Ma_GK07_AREPS_Topology_BuildingBlocks.gpmlz'],
                      'ValidTimeRange': [230., 0.]}

    elif MODEL_NAME == 'PALEOMAP':
        model_dict = {'RotationFile': ['PALEOMAP_PlateModel.rot'],
                      'Coastlines': 'PALEOMAP_coastlines.gpmlz',
                      'StaticPolygons': 'PALEOMAP_PlatePolygons.gpmlz',
                      'ValidTimeRange': [750., 0.]}

    elif MODEL_NAME == 'RODINIA2013':
        model_dict = {'RotationFile': ['Li_Rodinia_v2013.rot'],
                      'Coastlines': 'Li_Rodinia_v2013_Coastlines.gpmlz',
                      'StaticPolygons': 'Li_Rodinia_v2013_StaticPolygons.gpmlz',
                      'ValidTimeRange': [1100., 530.]}

    elif MODEL_NAME == 'GOLONKA':
        model_dict = {'RotationFile': ['Phanerozoic_EarthByte.rot'],
                      'Coastlines': 'Phanerozoic_EarthByte_Coastlines.gpmlz',
                      'StaticPolygons': 'Phanerozoic_EarthByte_ContinentalRegions.gpmlz',
                      'ValidTimeRange': [540., 0.]}

    elif MODEL_NAME == 'VH_VDM':
        model_dict = {'RotationFile': ['vanHinsbergen_master.rot'],
                      'Coastlines': 'Coastlines_Seton_etal_2012.gpmlz',
                      'StaticPolygons': 'Basis_Polygons_Seton_etal_2012.gpmlz',
                      'ValidTimeRange': [200., 0.]}

    elif MODEL_NAME == 'MATTHEWS2016':
        model_dict = {'RotationFile': ['Global_EB_250-0Ma_GK07_Matthews_etal.rot',
                                       'Global_EB_410-250Ma_GK07_Matthews_etal.rot'],
                      'Coastlines': 'Global_coastlines_2015_v1_low_res.gpmlz',
                      'StaticPolygons': 'PresentDay_StaticPlatePolygons_Matthews++.gpmlz',
                      'PlatePolygons': ['Global_EarthByte_Mesozoic-Cenozoic_plate_boundaries_Matthews_etal.gpmlz',
                                        'Global_EarthByte_Paleozoic_plate_boundaries_Matthews_etal.gpmlz',
                                        'TopologyBuildingBlocks_AREPS.gpmlz'],
                      'ValidTimeRange': [410., 0.]}

    elif MODEL_NAME == 'MATTHEWS2016_mantle_ref':
        model_dict = {
            "PlatePolygons": [
                "Matthews_etal_GPC_2016_Paleozoic_PlateTopologies.gpmlz",
                "Matthews_etal_GPC_2016_MesozoicCenozoic_PlateTopologies.gpmlz"],
            "RotationFile": [
                "Matthews_etal_GPC_2016_410-0Ma_GK07.rot"],
            "Coastlines": "Matthews_etal_GPC_2016_Coastlines.gpmlz",
            "ValidTimeRange": [
                410.0,
                0.0],
            "StaticPolygons": "Muller_etal_AREPS_2016_StaticPolygons.gpmlz"}

    elif MODEL_NAME == 'MATTHEWS2016_pmag_ref':
        model_dict = {
            "PlatePolygons": [
                "Matthews_etal_GPC_2016_Paleozoic_PlateTopologies_PMAG.gpmlz",
                "Matthews_etal_GPC_2016_MesozoicCenozoic_PlateTopologies_PMAG.gpmlz"],
            "RotationFile": [
                "Matthews_etal_GPC_2016_410-0Ma_GK07_PMAG.rot"],
            "Coastlines": "Matthews_etal_GPC_2016_Coastlines.gpmlz",
            "ValidTimeRange": [
                410.0,
                0.0],
            "StaticPolygons": "Muller_etal_AREPS_2016_StaticPolygons.gpmlz"}

    elif MODEL_NAME == 'DOMEIER2014':
        model_dict = {'RotationFile': ['LP_TPW.rot'],
                      'Coastlines': 'LP_land.shp',
                      'StaticPolygons': 'LP_land.shp',
                      'PlatePolygons': ['LP_ridge.gpml',
                                        'LP_subduction.gpml',
                                        'LP_transform.gpml',
                                        'LP_topos.gpml'],
                      'ValidTimeRange': [410., 250.]}

    elif MODEL_NAME == 'PEHRSSON2015':
        model_dict = {'RotationFile': ['T_Rot_Model_Abs_25Ma_20131004.rot'],
                      'Coastlines': 'PlatePolygons.shp',
                      'StaticPolygons': 'PlatePolygons.shp',
                      'ValidTimeRange': [2100., 1275.]}

    elif MODEL_NAME == 'SCOTESE&WRIGHT2018':
        model_dict = {'RotationFile': ['Scotese_2016_PALEOMAP_PlateModel.rot'],
                      'StaticPolygons': 'Scotese_2016_PALEOMAP_PlatePolygons.gpml',
                      'ValidTimeRange': [659., 0.]}

    else:
        # model_dict = 'Error: Model Not Listed'
        model_dict = None

    return model_dict