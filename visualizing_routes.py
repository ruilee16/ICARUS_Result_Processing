import arcpy, xml.etree.ElementTree as ET, sqlite3, re, os.path as path, datetime

start_time = datetime.datetime.now()
database = r'D:\Dropbox\Dropbox (ASU)\UAHS Team Share\Data\Simulation Results\run_20_04_09\database.db'
plans_xml = r'D:\Dropbox\Dropbox (ASU)\UAHS Team Share\Data\Simulation Results\run_20_04_09\output\output_plans.xml\output_plans.xml'
out_path = 'D:/2020/ICURAS/run_20_04_09'  # output folder
out_name = 'run_04_09_2020'  # output filename

# load database
conn = None
try:
    conn = sqlite3.connect(database)
except:
    print('database not exist')

# parse the .xml data
tree = ET.parse(plans_xml)
root = tree.getroot()


# extract trav_mode, trav_time, distance values for each link into .txt file.
def extract_links_value(route, value):
    temp_link = []
    for item in route:
        if item[1] == value:
            print(item[0])
            temp_link.append(item[0])
    return temp_link


# fetch node location from database
def fetch_node(nodes, conn):
    cur = conn.cursor()
    cur.execute(
        'SELECT links.link_id, point FROM nodes INNER JOIN links on links.source_node = nodes.node_id WHERE links.link_id in (%s)' % nodes)
    rows = dict(cur.fetchall())
    sort_order = nodes.split(',')
    node_loc = []
    for link in sort_order:
        temp = re.findall('\d+.\d+', rows['%s' % link])
        temp_float = [float(i) for i in temp]
        node_loc.append(temp_float)
    return node_loc



# check if file exist:
if path.exists(r'%s/%s' % (out_path, out_name)) is False:
    dataset = r'D:\Dropbox\Dropbox (ASU)\class\2020Spring\Hydrology\lab\lab1\Lab1Data\Data\Outlet.shp' # get coordinate system
    spatial_ref = arcpy.Describe(dataset).spatialReference  # create spatial reference for the output file
    arcpy.CreateFeatureclass_management(out_path=out_path, out_name=out_name, geometry_type='POLYLINE',
                                        spatial_reference=spatial_ref)  # create .shp file to hold the route
FID = 0
with open('%s/%s.txt' % (out_path, out_name), 'w') as f:
    f.write('FID,personID,ifSelect,legNum,trav_mode,trav_time,distance')
    f.write('\n')
    print('start Parsing')
    for person in root.findall('person'):
        icur = arcpy.da.InsertCursor(r'%s/%s.shp' % (out_path, out_name), 'SHAPE@')
        plan_num = 1
        for plan in person.findall('plan'):
            if plan.attrib['selected'] == 'yes':
                leg_num = 1  # number of leg in this plan
                for item in plan.findall('leg'):
                    route = item.find('route')
                    if route.text is None:
                        f.write('%s,%s,%s,%s,%s,%s,%s' % ('null', person.attrib['id'], plan.attrib['selected'], leg_num,
                                                    item.find('attributes').find('attribute').text, route.attrib['trav_time'],
                                                    route.attrib['distance']))
                    else:
                        nodes = item.find('route').text.replace(' ', ',')
                        temp = fetch_node(nodes, conn)
                        # draw points and lines based on the nodes x y coordinate
                        line = arcpy.Polyline(arcpy.Array([arcpy.Point(*coords) for coords in temp]))
                        icur.insertRow((line,))
                        # txt write: person id, plan number, plan selected, leg id.
                        route_info = '%s,%s,%s,%s,%s,%s,%s' % (FID, person.attrib['id'], plan.attrib['selected'], leg_num,
                                                               item.find('attributes').find('attribute').text, route.attrib['trav_time'],
                                                                route.attrib['distance'])
                        # print(route_info)
                        f.write(route_info)
                        f.write('\n')
                        FID += 1
                        leg_num += 1
                        if FID % 1000 == 0:
                            print('time past: %s\n' % (datetime.datetime.now()-start_time))
                            print(route_info)
    del icur
