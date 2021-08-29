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

import Grid from '@material-ui/core/Grid';
import { createStyles, Theme, WithStyles, withStyles } from '@material-ui/core/styles';
import Typography from '@material-ui/core/Typography';
import * as React from 'react';

import * as api from './api';
import ChunksProgressPanel from './ChunksProgressPanel';
import DottedProgress from './DottedProgress';
import EnginesProgressPanel from './EnginesProgressPanel';


const styles = (theme: Theme) => createStyles({
    root: {
        padding: theme.spacing(3),
    },
    titleGrid: {
        marginBottom: theme.spacing(2),
    },
    tableDottedProgress: {
        width: 360,
    },
});

interface Props extends WithStyles<typeof styles> {
    tableName: string
    tableProgress: api.TableProgress
    onChangeActiveTableProgress: (tableName?: string) => void
}

class TableProgressPage extends React.Component<Props> {
    componentDidMount() {
        this.props.onChangeActiveTableProgress(this.props.tableName);
    }

    componentWillUnmount() {
        this.props.onChangeActiveTableProgress(undefined);
    }

    render() {
        const { classes } = this.props;

        return (
            <div className={classes.root}>
                <Grid container justify='space-between' alignItems='baseline' className={classes.titleGrid}>
                    <Grid item>
                        <Typography variant='h3'>{this.props.tableName}</Typography>
                    </Grid>
                    <Grid item className={classes.tableDottedProgress}>
                        <DottedProgress total={api.TABLE_MAX_STEPS} status={this.props.tableProgress.Status} />
                    </Grid>
                </Grid>

                <EnginesProgressPanel tableProgress={this.props.tableProgress} />
                <ChunksProgressPanel tableProgress={this.props.tableProgress} />
            </div>
        )
    }
}

export default withStyles(styles)(TableProgressPage);
