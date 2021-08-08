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

import Card from '@material-ui/core/Card';
import CardContent from '@material-ui/core/CardContent';
import CardHeader from '@material-ui/core/CardHeader';
import { blueGrey, green, lime, red } from '@material-ui/core/colors';
import IconButton from '@material-ui/core/IconButton';
import LinearProgress from '@material-ui/core/LinearProgress';
import { createStyles, WithStyles, withStyles } from '@material-ui/core/styles';
import ChevronRightIcon from '@material-ui/icons/ChevronRight';
import * as fileSize from 'filesize';
import * as React from 'react';
import { Link } from 'react-router-dom';

import * as api from './api';
import ErrorButton from './ErrorButton';


const styles = createStyles({
    cardHeaderContent: {
        overflow: 'hidden',
    },
    card_notStarted: {
        backgroundColor: blueGrey[50],
    },
    card_running: {
        backgroundColor: lime[50],
    },
    card_succeed: {
        backgroundColor: green[50],
    },
    card_failed: {
        backgroundColor: red[50],
    },
    progressBar: {
        height: '1ex',
    },
});

const TABLE_NAME_REGEXP = /^`((?:[^`]|``)+)`\.`((?:[^`]|``)+)`$/;

interface Props extends WithStyles<typeof styles> {
    tableName: string
    tableInfo: api.TableInfo
}

class TableProgressCard extends React.Component<Props> {
    render() {
        const { classes } = this.props;

        const cardClass = api.classNameOfStatus(
            this.props.tableInfo,
            classes.card_notStarted,
            classes.card_running,
            classes.card_succeed,
            classes.card_failed,
        );

        let tbl: string, db: string;
        const m = this.props.tableName.match(TABLE_NAME_REGEXP);
        if (m) {
            db = m[1].replace(/``/g, '`');
            tbl = m[2].replace(/``/g, '`');
        } else {
            db = '';
            tbl = this.props.tableName;
        }

        const progress = this.props.tableInfo.w * 100 / this.props.tableInfo.z;
        const progressTitle = `Transferred to Importer: ${fileSize(this.props.tableInfo.w)} / ${fileSize(this.props.tableInfo.z)}`;

        return (
            <Card className={cardClass} title={this.props.tableName}>
                <CardHeader
                    classes={{ content: classes.cardHeaderContent }}
                    title={tbl}
                    subheader={db}
                    titleTypographyProps={{ variant: 'body1', noWrap: true }}
                    subheaderTypographyProps={{ variant: 'body2', noWrap: true }}
                    action={
                        <>
                            {this.props.tableInfo.m && <ErrorButton lastError={this.props.tableInfo.m} />}
                            <IconButton component={Link} to={`/table?t=${encodeURIComponent(this.props.tableName)}`}>
                                <ChevronRightIcon />
                            </IconButton>
                        </>
                    }
                />
                <CardContent>
                    <LinearProgress
                        className={classes.progressBar}
                        variant='determinate'
                        value={progress}
                        title={progressTitle}
                    />
                </CardContent>
            </Card>
        );
    }
}

export default withStyles(styles)(TableProgressCard);
