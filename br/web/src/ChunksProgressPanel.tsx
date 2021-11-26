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

import ExpansionPanel from '@material-ui/core/ExpansionPanel';
import ExpansionPanelDetails from '@material-ui/core/ExpansionPanelDetails';
import ExpansionPanelSummary from '@material-ui/core/ExpansionPanelSummary';
import LinearProgress from '@material-ui/core/LinearProgress';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import * as React from 'react';

import * as api from './api';


interface Props {
    tableProgress: api.TableProgress
}

interface Chunk {
    key: string
    engineID: number
    read: number
    total: number
}

function sortKey(chunk: Chunk): number {
    if (chunk.read > 0 && chunk.read < chunk.total) {
        return chunk.read / chunk.total;
    } else if (chunk.read <= 0) {
        return 2;
    } else {
        return 3;
    }
}


export default class ChunksProgressPanel extends React.Component<Props> {
    render() {
        let files: Chunk[] = [];
        for (let engineID in this.props.tableProgress.Engines) {
            for (const progress of this.props.tableProgress.Engines[engineID].Chunks) {
                files.push({
                    key: `${progress.Key.Path}:${progress.Key.Offset}`,
                    engineID: +engineID,
                    read: progress.Chunk.Offset - progress.Key.Offset,
                    total: progress.Chunk.EndOffset - progress.Key.Offset,
                });
            }
        }
        files.sort((a, b) => {
            const aSortKey = sortKey(a);
            const bSortKey = sortKey(b);
            if (aSortKey < bSortKey) {
                return -1;
            } else if (aSortKey > bSortKey) {
                return 1;
            } else if (a.key < b.key) {
                return -1;
            } else {
                return +(a.key > b.key);
            }
        });

        return (
            <ExpansionPanel>
                <ExpansionPanelSummary>
                    Files
                </ExpansionPanelSummary>
                <ExpansionPanelDetails>
                    <Table>
                        <TableHead>
                            <TableRow>
                                <TableCell>Chunk</TableCell>
                                <TableCell>Engine</TableCell>
                                <TableCell>Progress</TableCell>
                            </TableRow>
                        </TableHead>
                        <TableBody>
                            {files.map(chunk => (
                                <TableRow key={chunk.key}>
                                    <TableCell component='th' scope='row'>
                                        {chunk.key}
                                    </TableCell>
                                    <TableCell>
                                        :{chunk.engineID}
                                    </TableCell>
                                    <TableCell>
                                        <LinearProgress
                                            value={chunk.read * 100 / chunk.total}
                                            variant='determinate'
                                        />
                                    </TableCell>
                                </TableRow>
                            ))}
                        </TableBody>
                    </Table>
                </ExpansionPanelDetails>
            </ExpansionPanel>
        );
    }
}
