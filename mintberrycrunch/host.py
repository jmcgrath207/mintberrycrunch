class Host:

    def __init__(self, app):
        self.app = app

    async def _ssh_build_connection(self):
        pass

    async def build_host_lookup():
        for key, value in app.tasks.items():
            if not bool(app.connection_manager.get(key)):
                app.connection_manager[value['conn_type']] = set()
            app.connection_manager[value['conn_type']].add(value['host_group'])

        temp_dic = {}
        for key, value in app.connection_manager.items():
            for host_group in value:
                for host_name, host_address in app.hosts.get(host_group).items():
                    temp_dic[host_name][key] = {}
                    temp_dic[host_name][key]["host_address"] = host_address