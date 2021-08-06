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

import Button from '@material-ui/core/Button';
import Dialog from '@material-ui/core/Dialog';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from '@material-ui/core/DialogTitle';
import IconButton from '@material-ui/core/IconButton';
import Portal from '@material-ui/core/Portal';
import Snackbar from '@material-ui/core/Snackbar';
import SnackbarContent from '@material-ui/core/SnackbarContent';
import { createStyles, Theme, WithStyles, withStyles } from '@material-ui/core/styles';
import TextField from '@material-ui/core/TextField';
import AddIcon from '@material-ui/icons/Add';
import CloseIcon from '@material-ui/icons/Close';
import CloudUploadIcon from '@material-ui/icons/CloudUpload';
import * as React from 'react';


const styles = (theme: Theme) => createStyles({
    leftIcon: {
        marginRight: theme.spacing(1),
    },
    uploadButton: {
        marginBottom: theme.spacing(3),
    },
    errorSnackBar: {
        background: theme.palette.error.dark,
    },
});

interface Props extends WithStyles<typeof styles> {
    onSubmitTask: (taskCfg: string) => Promise<void>
}

interface States {
    dialogOpened: boolean
    errorOpened: boolean
    errorMessage: string
    taskConfig: string
}

class TaskButton extends React.Component<Props, States> {
    constructor(props: Props) {
        super(props);

        this.state = {
            dialogOpened: false,
            errorOpened: false,
            errorMessage: '',
            taskConfig: '',
        };
    }

    handleOpenDialog = () => this.setState({ dialogOpened: true });

    handleCloseDialog = () => this.setState({ dialogOpened: false });

    handleCloseError = () => this.setState({ errorOpened: false });

    handleUploadFile = (e: React.ChangeEvent<HTMLInputElement>) => {
        const files = e.currentTarget.files;
        if (files === null) {
            return;
        }

        const reader = new FileReader();
        reader.onload = (e: any) => this.setState({ taskConfig: e.target.result });
        reader.readAsText(files[0]);
    };

    handleChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => this.setState({ taskConfig: e.target.value });

    handleSubmitTask = async () => {
        try {
            await this.props.onSubmitTask(this.state.taskConfig);
            this.handleCloseDialog();
        } catch (e) {
            this.setState({ errorOpened: true, errorMessage: '' + e });
        }
    };

    render() {
        const { classes } = this.props;

        return (
            <div>
                <IconButton onClick={this.handleOpenDialog} color='inherit' title='Submit new task'>
                    <AddIcon />
                </IconButton>
                <Dialog open={this.state.dialogOpened} onClose={this.handleCloseDialog} fullWidth maxWidth='sm' disableBackdropClick>
                    <DialogTitle>Submit task</DialogTitle>
                    <DialogContent>
                        <div className={classes.uploadButton}>
                            <input accept='.toml' hidden id='upload-task-config' type='file' onChange={this.handleUploadFile} />
                            <label htmlFor='upload-task-config'>
                                <Button component='span' variant='contained' >
                                    <CloudUploadIcon className={classes.leftIcon} />
                                    Upload
                                </Button>
                            </label>
                        </div>
                        <TextField label='Task configuration' multiline fullWidth rows='20' value={this.state.taskConfig} onChange={this.handleChange} />
                    </DialogContent>
                    <DialogActions>
                        <Button onClick={this.handleCloseDialog} color='primary'>
                            Cancel
                        </Button>
                        <Button onClick={this.handleSubmitTask} color='secondary' disabled={this.state.taskConfig.length === 0}>
                            Submit
                        </Button>
                    </DialogActions>
                </Dialog>
                <Portal> {/* the Portal workarounds mui-org/material-ui#12201 */}
                    <Snackbar open={this.state.errorOpened} autoHideDuration={5000} onClose={this.handleCloseError}>
                        <SnackbarContent className={classes.errorSnackBar} message={this.state.errorMessage} action={
                            <IconButton color='inherit' onClick={this.handleCloseError}>
                                <CloseIcon />
                            </IconButton>
                        } />
                    </Snackbar>
                </Portal>
            </div>
        )
    }
}

export default withStyles(styles)(TaskButton);
