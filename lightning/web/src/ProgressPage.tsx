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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import Chip from '@material-ui/core/Chip';
import ExpansionPanel from '@material-ui/core/ExpansionPanel';
import ExpansionPanelDetails from '@material-ui/core/ExpansionPanelDetails';
import ExpansionPanelSummary from '@material-ui/core/ExpansionPanelSummary';
import GridList from '@material-ui/core/GridList';
import GridListTile from '@material-ui/core/GridListTile';
import { createStyles, Theme, WithStyles, withStyles } from '@material-ui/core/styles';
import Typography from '@material-ui/core/Typography';
import * as React from 'react';

import * as api from './api';
import TableProgressCard from './TableProgressCard';


const styles = (theme: Theme) => createStyles({
    root: {
        padding: theme.spacing(3),
    },
    gridList: {
        width: '100%',
    },
    panelTitle: {
        flexGrow: 1,
    },
});

interface Props extends WithStyles<typeof styles> {
    taskProgress: api.TaskProgress
}

interface ExpansionPanelProps extends Props {
    status: api.TaskStatus
    title: string
    defaultExpanded?: boolean
}

class TableExpansionPanel extends React.Component<ExpansionPanelProps> {
    render() {
        const { classes } = this.props;

        let tables: [string, api.TableInfo][] = [];
        let hasAnyError = false;
        for (let tableName in this.props.taskProgress.t) {
            const tableInfo = this.props.taskProgress.t[tableName];
            if (tableInfo.s === this.props.status) {
                tables.push([tableName, tableInfo]);
                if (tableInfo.m) {
                    hasAnyError = true;
                }
            }
        }
        tables.sort((a, b) => {
            // first sort by whether an error message exists (so errored tables
            // appeared first), then sort by table name.
            if (a[1].m && !b[1].m) {
                return -1;
            } else if (b[1].m && !a[1].m) {
                return 1;
            } else if (a[0] < b[0]) {
                return -1;
            } else {
                return +(a[0] > b[0]);
            }
        });

        // TODO: This is not yet responsive.
        const cols = Math.ceil(window.innerWidth / 300);

        return (
            <ExpansionPanel defaultExpanded={this.props.defaultExpanded}>
                <ExpansionPanelSummary>
                    <Typography className={classes.panelTitle} variant='h5'>{this.props.title}</Typography>
                    <Chip label={tables.length} color={hasAnyError ? 'secondary' : 'default'} />
                </ExpansionPanelSummary>
                <ExpansionPanelDetails>
                    <GridList className={classes.gridList} cols={cols} cellHeight={132}>{
                        tables.map(([tableName, tableInfo]) => (
                            <GridListTile key={tableName}>
                                <TableProgressCard tableName={tableName} tableInfo={tableInfo} />
                            </GridListTile>
                        ))
                    }</GridList>
                </ExpansionPanelDetails>
            </ExpansionPanel>
        );
    }
}

class ProgressPage extends React.Component<Props> {
    render() {
        const { classes } = this.props;

        return (
            <div className={classes.root}>
                <TableExpansionPanel defaultExpanded status={api.TaskStatus.Running} title='Active' {... this.props} />
                <TableExpansionPanel defaultExpanded status={api.TaskStatus.Completed} title='Completed' {... this.props} />
                <TableExpansionPanel status={api.TaskStatus.NotStarted} title='Pending' {... this.props} />
            </div>
        );
    }
}

export default withStyles(styles)(ProgressPage);
