// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

import IconButton from '@material-ui/core/IconButton';
import ListSubheader from '@material-ui/core/ListSubheader';
import Menu from '@material-ui/core/Menu';
import MenuItem from '@material-ui/core/MenuItem';
import MenuList from '@material-ui/core/MenuList';
import RefreshIcon from '@material-ui/icons/Refresh';
import * as React from 'react';


const AUTO_REFRESH_INTERVAL_KEY = 'autoRefreshInterval';

interface Props {
    onRefresh: () => Promise<void>
}

interface States {
    isRefreshing: boolean
    menuOpened: boolean
    autoRefreshInterval: number
}

export default class RefreshButton extends React.Component<Props, States> {
    private ref: React.RefObject<HTMLButtonElement>;
    private autoRefreshTimer?: number;

    constructor(props: Props) {
        super(props);

        this.ref = React.createRef();

        this.state = {
            isRefreshing: false,
            menuOpened: false,
            autoRefreshInterval: 0,
        };
    }

    async refresh() {
        this.setState({ isRefreshing: true });
        await this.props.onRefresh();
        this.setState({ isRefreshing: false });
    }

    changeInterval(interval: number) {
        this.setState({ autoRefreshInterval: interval });
        localStorage.setItem(AUTO_REFRESH_INTERVAL_KEY, '' + interval);

        clearInterval(this.autoRefreshTimer);
        this.autoRefreshTimer = (interval > 0) ?
            window.setInterval(() => this.refresh(), interval * 1000) :
            undefined;
    }

    handleCloseMenu = () => {
        this.setState({ menuOpened: false });
    }

    handleRefresh = () => {
        this.handleCloseMenu();
        this.refresh();
    }

    handleToggleMenu = () => {
        this.setState(state => ({ menuOpened: !state.menuOpened }));
    }

    handleChangeInterval = (interval: number) => () => {
        this.handleCloseMenu();
        this.changeInterval(interval);
    }

    async componentDidMount() {
        await this.refresh();

        const autoRefreshInterval = (localStorage.getItem(AUTO_REFRESH_INTERVAL_KEY) as any) | 0;
        this.changeInterval(autoRefreshInterval);
    }

    componentWillUnmount() {
        clearInterval(this.autoRefreshTimer);
    }

    render() {
        return (
            <div>
                <IconButton ref={this.ref} title='Refresh' color='inherit' onClick={this.handleToggleMenu}>
                    <RefreshIcon />
                </IconButton>
                <Menu anchorEl={this.ref.current} open={this.state.menuOpened} onClose={this.handleCloseMenu}>
                    <MenuList>
                        <MenuItem disabled={this.state.isRefreshing} onClick={this.handleRefresh} divider>
                            Refresh now
                        </MenuItem>
                        <ListSubheader>
                            Auto refresh
                        </ListSubheader>
                        <MenuItem onClick={this.handleChangeInterval(2)} selected={this.state.autoRefreshInterval === 2}>
                            2 seconds
                        </MenuItem>
                        <MenuItem onClick={this.handleChangeInterval(300)} selected={this.state.autoRefreshInterval === 300}>
                            5 minutes
                        </MenuItem>
                        <MenuItem onClick={this.handleChangeInterval(0)} selected={this.state.autoRefreshInterval === 0}>
                            Off
                        </MenuItem>
                    </MenuList>
                </Menu>
            </div>
        );
    }
}
